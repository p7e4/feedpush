#!/usr/bin/env python3
import argparse
import asyncio
import aiohttp
import hashlib
import logging
import rssfeed
import tomllib
import base64
import shelve
import hmac
import time
import re
import os

__version__ = "0.2.0"

logging.basicConfig(format="[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%m-%d %H:%M:%S", level=logging.INFO)
logger = logging.getLogger("feedpush")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

TagRegx = re.compile(r"<.+?>")
Opml = re.compile(r'xmlUrl="(.+?)"')

async def main(conf, db):
    nextrun = time.time()
    while True:
        lastrun = int(time.time())
        async with aiohttp.ClientSession(headers=headers) as session:
            for item in conf["project"]:
                contents = list()
                webhooks = [{"url": i} if isinstance(i, str) else i for i in item["webhooks"]]
                feeds = [{"url": i} if isinstance(i, str) else i for i in item["feeds"]]
                async with asyncio.TaskGroup() as tg:
                    for feed in feeds:
                        tg.create_task(worker(db, session, feed, contents))

                if not contents: continue
                async with asyncio.TaskGroup() as tg:
                    for webhook in webhooks:
                        tg.create_task(sendMsg(session, webhook, contents))

        db["lastrun"] = lastrun
        if conf.get("interval", 30) <= 0: break
        nextrun += conf.get("interval", 15) * 60
        if (sleep:=nextrun - int(time.time())) > 0:
            logger.info(f"sleep {sleep}min for next run")
            await asyncio.sleep(sleep)


async def worker(db, session, feed, contents):
    try:
        async with session.get(feed["url"], ssl=False) as resp:
            feed = rssfeed.parse(await resp.text()) | feed
    except Exception as e:
        return logger.error(f"{feed["url"]}: {e}")

    result = [feed["name"]]
    for item in feed["items"]:
        if not db["his"].get(item["url"]):
            db["his"][item["url"]] = True
            result.append((item["title"], item["url"]))

    if not db["lastrun"] and len(result) > 6: result = result[:6]
    if len(result) > 1: contents.extend(result)


async def sendMsg(session, webhook, contents):
    async def send(json):
        try:
            async with session.post(webhook["url"], params=params, json=json) as resp:
                if not resp.headers.get("Content-Type").startswith("application/json") or (await resp.json()).get(exceptCode) != 0:
                    raise ValueError(await resp.text())
        except Exception as e:
            logger.error(f"send msg error\n{json=}\n{webhook["url"]=}\n{e}")

    timestamp = int(time.time())
    exceptCode = "errcode"
    params = dict()
    # https://open.feishu.cn/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN
    # 100次/分钟, 5次/秒, 请求体 < 20KB
    if webhook["url"].startswith("https://open.feishu.cn/open-apis/bot/v2/hook/"):
        exceptCode = "code"
        if webhook.get("sign"):
            webhook["sign"] = base64.b64encode(hmac.new(f"{timestamp}\n{webhook["sign"]}".encode(), digestmod=hashlib.sha256).digest()).decode()
        messages = [dict(tag="text", text=f"{i}\n") if isinstance(i, str) else dict(tag="a", text=f"{i[0]}\n", href=i[1]) for i in contents]
        await send({
            "timestamp": timestamp,
            "sign": webhook.get("sign"),
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "content": [messages]
                    }
                }
            }
        })


    # https://developer.work.weixin.qq.com/document/path/91770
    # 20条/分钟, 消息内容不超过4096个字节
    elif webhook["url"].startswith("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="):
        messages = [f"### {i}" if isinstance(i, str) else f"[{i[0]}]({i[1]})" for i in contents]
        for message in textSplit("\n".join(messages), 4096):
            await send({
               "msgtype": "markdown",
               "markdown": {
                    "content": message
                }
            })

    # https://open.dingtalk.com/document/orgapp/custom-robot-access
    # 20条/分钟
    elif webhook["url"].startswith("https://oapi.dingtalk.com/robot/send?access_token="):
        if webhook.get("sign"):
            sign = hmac.new(webhook["sign"].encode(), f"{timestamp * 1000}\n{webhook["sign"]}".encode(), digestmod=hashlib.sha256).digest()
            params = {"timestamp": timestamp * 1000, "sign": base64.b64encode(sign).decode()}

        messages = [f"### {i}" if isinstance(i, str) else f"[{i[0]}]({i[1]})" for i in contents]
        for message in textSplit("\n".join(messages), unknown):
            await send({
               "msgtype": "markdown",
               "markdown": {
                    "title": message,
                    "text": message
                }
            })
    else:
        logger.error(f"unsupported webhook: {webhook}")


def textSplit(text, length):
    if len(content:=text.encode()) <= length:
        return [text]

    if text.find("\n") == -1:
        length = length // 4
        return [text[i:i+length] for i in range(0, len(text), length)]

    result = list()
    while len(content) > length:
        sp = content.rfind("\n", 0, length)
        result.append(content[:sp].decode())
        content = content[sp+1:]
    else:
        result.append(content.decode())
        return result

def cli():
    parser = argparse.ArgumentParser(prog="feedpush")
    parser.add_argument("-d", help="workdir dir", default="./", dest="workdir")
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args()
    confFile = os.path.join(args.workdir, "conf.toml")
    if not os.path.exists(confFile):
        exit(f"[-] could not find conf file `{confFile}`")
    with open(confFile, "rb") as f:
        try:
            conf = tomllib.load(f)
        except tomllib.TOMLDecodeError:
            exit(f"[-] conf file `{confFile}` parsing error!")

    db = shelve.open(os.path.join(args.workdir, "db"), protocol=5, writeback=True)
    if not db.get("his"):
        db["his"] = dict()
        db["lastrun"] = 0
    try:
        asyncio.run(main(conf, db))
    finally:
        db.sync()
        db.close()

if __name__ == "__main__":
    cli()
