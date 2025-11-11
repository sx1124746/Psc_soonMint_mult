import queue
import sys
import time
import json
from typing import Dict, Any
import base64
import requests
from eth_account import Account
from eth_account.messages import encode_typed_data
import threading
from concurrent.futures import ThreadPoolExecutor
import signal
from loguru import logger


# 全局队列和标志
task_queue = queue.Queue(maxsize=100)
is_running = True
lock = threading.Lock()  


def signature(private_key):
    account = Account.from_key(private_key)
    from_address = account.address
    account2 = Account.create()
    nonce = str(account2._key_obj)
    current_timestamp = int(time.time())

    tx = {
        "from": from_address,
        "to": "0x0FE812a6BA666284e0c414646e694a53F1409393",
        "value": "1000000",
        "validAfter": str(current_timestamp - 600),
        "validBefore": str(current_timestamp + 60),
        "nonce": nonce
    }

    message = {
        "types": {
            "TransferWithAuthorization": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "validAfter", "type": "uint256"},
                {"name": "validBefore", "type": "uint256"},
                {"name": "nonce", "type": "bytes32"}
            ]
        },
        "domain": {
            "name": "USD Coin",
            "version": "2",
            "chainId": 8453,
            "verifyingContract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        },
        "primaryType": "TransferWithAuthorization",
        "message": tx
    }
    signable_message = encode_typed_data(full_message=message)
    signed = Account.sign_message(signable_message, private_key)

    signature_data = '0x' + signed.signature.hex()

    x_payment = {
        "x402Version": 1,
        "scheme": "exact",
        "network": "base",
        "payload": {
            "signature": signature_data,
            "authorization": tx
        }
    }
    x_payment_str:  str = json.dumps(x_payment)
    ut_x_payment = base64.b64encode(x_payment_str.encode('utf-8')).decode('ascii')
    return ut_x_payment


def create_task(task_id, **kwargs) -> Dict[str, Any]:
    """
    生成者函数
    :param task_id: 任务ID
    :param kwargs:  看需要
    :return:
    """
    task = {
        "task_id": task_id,
        "url": kwargs.get('url'),
        "headers": {
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'access-control-expose-headers': 'X-PAYMENT-RESPONSE',
            'cache-control': 'no-cache',
            'content-type': 'text/plain;charset=UTF-8',
            'origin': 'https://10ssoon.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://10ssoon.com/',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'x-payment': signature(kwargs.get('private_key'))
        },
        "data": {
            "direction": kwargs.get('type')
        }
    }

    return task


def producer() -> None:
    """生产者函数 - 纯同步实现"""
    task_id = 0
    while True:
        with lock:
            if not is_running:
                break

        # 签名+up
        parameters1: Dict[str, str] = {
            "url": "https://api.10ssoon.com/payment/bet",
            "private_key": "",
            "type": "up"
        }
        task = create_task(task_id=task_id, **parameters1)
        task_queue.put(task)
        logger.info(f"Sign | ID : {task_id} | Type：{task['data']['direction']} | ApartSign：{task['headers']['x-payment'][150:170]}")
        task_id += 1

        # 签名+down
        parameters2: Dict[str, str] = {
            "url": "https://api.10ssoon.com/payment/bet",
            "private_key": "",
            "type": "down"
        }
        task2 = create_task(task_id=task_id, **parameters2)
        task_queue.put(task2)
        logger.info(f"Sign | ID : {task_id} | Type：{task2['data']['direction']} | ApartSign：{task2['headers']['x-payment'][150:170]}")
        task_id += 1

        time.sleep(0.5)  # 秒任务


def consumer(max_workers_num) -> None:
    """消费者函数 - 线程池异步处理"""
    with ThreadPoolExecutor(max_workers=max_workers_num) as executor:
        while True:
            with lock:
                running = is_running
            if not running and task_queue.empty():
                break
            try:
                task = task_queue.get(timeout=1)
                task_queue.task_done()
                executor.submit(process_request, task)
            except queue.Empty:
                continue


def process_request(task) -> None:
    """实际处理HTTP请求"""
    try:
        response = requests.post(
            task['url'],
            headers=task['headers'],
            data=task['data'],
            timeout=10
        )
        response.raise_for_status()
        result = {
            'task': task,
            'result': response.json(),
        }
        logger.success(f"Reqs | ID : {result['task']['task_id']} | Type: {result['task']['data']['direction']} | Result：{result['result']}")
    except requests.exceptions.HTTPError as e:
        result = {
            'task': task,
            'error': str(e),
        }
        # result_queue.put(result)
        if e.response is not None:
            if e.response.status_code == 402:
                logger.warning(f"Reqs | ID : {result['task']['task_id']} | Type: {result['task']['data']['direction']} | Error：{result['error']}")
            else:
                logger.error(f"Reqs | ID : {result['task']['task_id']} | Type: {result['task']['data']['direction']} | Error：{result['error']}")
        else:
            logger.error(f"Reqs | ID : {result['task']['task_id']} | Type: {result['task']['data']['direction']} | Result：{result['result']}")


def shutdown_handler(signum, frame):
    global is_running
    print("\n接收到终止信号，所有线程正在关闭...")
    with lock:
        is_running = False


def main():
    """主函数"""
    # 线程监听
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # 签名线程
    producer_thread = threading.Thread(target=producer)
    producer_thread.daemon = True
    producer_thread.start()

    # 402线程
    consumer_thread = threading.Thread(target=consumer, args=(10,))
    consumer_thread.daemon = True
    consumer_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
