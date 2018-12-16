import os
import json
import requests
import datetime
import boto3
from boto3.dynamodb.conditions import Key
import decimal
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DynamoDB:
    
    AWS_BOTO3_ACCESS_KEY = os.environ["AWS_BOTO3_ACCESS_KEY"]
    AWS_BOTO3_SECRET_KEY = os.environ["AWS_BOTO3_SECRET_KEY"]
    AWS_REGION = "ap-northeast-1"

    def __init__(self):
        
        self.con = boto3.resource(
                     'dynamodb',
                     aws_access_key_id=self.AWS_BOTO3_ACCESS_KEY,
                     aws_secret_access_key=self.AWS_BOTO3_SECRET_KEY,
                     region_name=self.AWS_REGION)

    def convert_to_decimal(self, kv_dict):
        
        return json.loads(json.dumps(kv_dict), parse_float=decimal.Decimal)
            
    def get(self, table_name, kv_dict):
        
        table = self.con.Table(table_name)
        res = table.get_item(Key=kv_dict)        
        logger.info("[DYNAMO_GET]:{{tbl:{},query:{}}}".format(table_name, kv_dict))
        return res.get("Item")

    def put(self, table_name, kv_dict):
        
        table = self.con.Table(table_name)
        item = self.convert_to_decimal(kv_dict)
        logger.info("[DYNAMO_PUT]:{{table:{},item:{}}}".format(table_name, item))
        table.put_item(Item=item)

    def delete(self, table_name, kv_dict):

        table = self.con.Table(table_name)
        res = table.delete_item(Key=kv_dict)
        logger.info("[DYNAMO_DELETE]:{{tbl:{},query:{}}}".format(table_name, kv_dict))

    def batch_delete(self, table_name, key, value):

        table = self.con.Table(table_name)
        items = self.query(table_name, key, value)
        if len(items) ==0:
            return

        key_names = [x["AttributeName"] for x in table.key_schema]
        delete_keys = [{k:v for k,v in x.items() if k in key_names} for x in items]
        with table.batch_writer() as batch:
            for key in delete_keys:
                batch.delete_item(Key=key)
        logger.info("[DYNAMO_BATCH_DELETE]:{{tbl_name:{},items:{}}}".format(table_name,items))

    def query(self, table_name, key, value):

        table = self.con.Table(table_name)
        res = table.query(KeyConditionExpression=Key(key).eq(value))
        items = res["Items"]
        logger.info("[DYNAMO_QUERY]:{{tbl:{},query:{},result_cnt:{}}}".format(table_name, {key:value}, len(items)))
        return items
      
    def update(self, table_name, query_key, query_value, update_key, update_value):
        
        table = self.con.Table(table_name)
        
        res = table.update_item(
            Key={query_key: query_value},
            UpdateExpression="set {} = :c".format(update_key),
            ExpressionAttributeValues={
                    ":c": update_value
            },
            ReturnValues="UPDATED_NEW"
        )
        
        status_code = res["ResponseMetadata"]["HTTPStatusCode"]
        request_id = res["ResponseMetadata"]["RequestId"]

        if status_code != 200:
            logger.error("[ERROR]:DynamoDB response is not 200. RequestedId is {}.".format(request_id))

        logger.info("[DYNAMO_UPDATE]:{{tbl:{},{}:{},{}:{}}}".format(table_name, query_key, query_value, update_key, update_value))


class Line:

    URL_REPLY = 'https://api.line.me/v2/bot/message/reply'
    URL_PUSH = 'https://api.line.me/v2/bot/message/push'
    HEADERS = {
        'Authorization': 'Bearer ' + os.environ['LINE_CHANNEL_ACCESS_TOKEN'],
        'Content-type': 'application/json'
    }

    def create_message_data(self, text):

        return {
            "messages": [
                {
                    "type": "text",
                    "text": text
                }
            ]
        }
      
    def reply(self, token, message):
        
        logger.info("[LINE_REPLY]:{}".format(message))
        data = self.create_message_data(message)
        data["replyToken"] = token
        requests.post(self.URL_REPLY, data=json.dumps(data), headers=self.HEADERS)

    def push(self, to, message):    
        
        logger.info("[LINE_PUSH]:{{to:{},message:{}}}".format(to, message))
        data = self.create_message_data(message)
        data["to"] = to
        requests.post(self.URL_PUSH, data=json.dumps(data), headers=self.HEADERS)


class Docomo:    
    
    APIKEY = os.environ["DOCOMO_APIKEY"]
    CHAT_ENDPOINT = "https://api.apigw.smt.docomo.ne.jp/naturalChatting/v1/dialogue?APIKEY={}".format(os.environ["DOCOMO_APIKEY"])
    REGISTER_ENDPOINT = "https://api.apigw.smt.docomo.ne.jp/naturalChatting/v1/registration?APIKEY={}".format(os.environ["DOCOMO_APIKEY"])

    def __init__(self, dynamo, table_type, record):
        self.dynamo = dynamo
        self.table_type = table_type
        self.record = record
        self.__set_docomo_id()
        
    def __set_docomo_id(self):

        if "docomo_id" in self.record:
            self.docomo_id = self.record["docomo_id"]
        else:
            self.docomo_id = self.register_docomo_id()
        
    def register_docomo_id(self):
        
        headers = {"Content-type": "application/json"}
        payload = {"botId":"Chatting", "appKind":"Smart Phone"}
           
        res = requests.post(self.REGISTER_ENDPOINT, data=json.dumps(payload), headers=headers)
        data = res.json()

        docomo_id = data["appId"]
        if self.table_type == "user":
            self.dynamo.update("gs_m_user", "line_mid", self.record["line_mid"], "docomo_id", docomo_id)
        elif self.table_type == "group":
            self.dynamo.update("gs_m_group", "group_id", self.record["group_id"], "docomo_id", docomo_id)            
       
        return docomo_id
        
    def chat(self, text):

        if "docomo_send_time" in self.record:
            docomo_recv_time = self.record["docomo_send_time"]
        else:
            docomo_recv_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        docomo_send_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        headers = {"Content-type": "application/json"}
        payload = {"language":"ja-JP","botId":"Chatting","appId":self.docomo_id,
                   "voiceText":text,"appRecvTime":docomo_recv_time,
                   "appSendTime":docomo_send_time}
           
        res = requests.post(self.CHAT_ENDPOINT, data=json.dumps(payload), headers=headers)
        data = res.json()
        
        docomo_send_time = data["serverSendTime"]
        system_text = data["systemText"]["utterance"]
        
        if self.table_type == "user":
            self.dynamo.update("gs_m_user", "line_mid", self.record["line_mid"], "docomo_send_time", docomo_send_time)
        elif self.table_type == "group":
            self.dynamo.update("gs_m_group", "group_id", self.record["group_id"], "docomo_send_time", docomo_send_time)

        return system_text     
      

def lambda_handler(event, context):

    logger.info(event)

    # from Line Post
    events = event.get("events")
    if events:        
        for ev in events:
            line_event_handler(ev) 
        return

def line_event_handler(event):
    
    if event["type"] != "message":
        return

    _type = event["source"]["type"]

    if _type == "user":
        handle_user_text(event)

    elif _type == "group":
        handle_group_text(event)

def is_command(text):
    return text.replace(" ","").replace("　","").replace("＠","@")[0] == "@"

def is_valid_command(text):
    commands = text.replace("＠","").replace("@","").replace("　"," ").split(" ")
    logger.info("[INPUT_COMMANDS]:{{commands:{}}}".format(commands))
    if commands[0] in ["help", "ls", "total", "initialize"]:
        if len(commands) == 1:
            return True
    elif commands[0] == "r":
        if len(commands) == 4 and commands[3].isnumeric():
            return True
    elif commands[0] == "rm":
        if len(commands) == 1:
            return True
        if len(commands) == 2 and commands[1].isnumeric():
            return True
    return False

def handle_command(text, dynamo, group_id):

    def get_sorted_item(dynamo, group_id):
        items = dynamo.query("gs_t_purchase", "group_id", group_id)
        tmp_dict = {item["created_at"]:item for item in items}
        return [tmp_dict[key] for key in sorted(tmp_dict)]

    commands = text.replace("＠","").replace("@","").replace("　"," ").split(" ")
    if commands[0] == "help":
        out_message = """[コマンド一覧]
・r <登録者名> <科目名> <金額>:登録する
・ls:リスト表示
・rm:直近のデータ削除
・rm <番号>:リスト番号のデータ削除
・total:合計表示
・initialize:データ初期化
"""
    elif commands[0] == "r":
        dynamo.put("gs_t_purchase", {"group_id":group_id,
                                     "created_at":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                     "name":commands[1],
                                     "item":commands[2],
                                     "cost":int(commands[3]),
                                     })
        out_message = "登録したよ。"

    elif commands[0] == "ls":
        sorted_items = get_sorted_item(dynamo, group_id)
        if len(sorted_items) == 0:
            out_message = "まず登録してね。"
        else:
            res_list = ["{}:{}/{}/{}".format(i, item["name"], item["item"], item["cost"]) for i, item in enumerate(sorted_items, 1)]
            out_message = "はい。\n" + "\n".join(res_list)

    elif commands[0] == "rm":
        sorted_items = get_sorted_item(dynamo, group_id)
        if len(sorted_items) == 0:
            out_message = "何も登録されていないから消せないよ。"

        elif len(commands) == 1:
            target_item = sorted_items[-1]
            dynamo.delete("gs_t_purchase", {"group_id":group_id,
                                            "created_at":target_item["created_at"],
                                            })
            out_message = "これを消したよ。\n{}:{}/{}/{}".format(len(sorted_items), target_item["name"], target_item["item"], target_item["cost"])            

        elif len(commands) == 2:
            target_num = int(commands[1])
            if target_num < 1 or target_num > len(sorted_items):
                out_message = "{}番はリストに存在しないから消せないよ。".format(commands[1])
            else:
                target_item = sorted_items[target_num-1]
                dynamo.delete("gs_t_purchase", {"group_id":group_id,
                                                "created_at":target_item["created_at"],
                                                })
                out_message = "これを消したよ。\n{}:{}/{}/{}".format(target_num, target_item["name"], target_item["item"], target_item["cost"])            

    elif commands[0] == "total":
        sorted_items = get_sorted_item(dynamo, group_id)
        if len(sorted_items) == 0:
            out_message = "まず登録してね。"
        else:
            res_dict = {}
            for item in sorted_items:
                if item["name"] not in res_dict:
                    res_dict[item["name"]] = int(item["cost"])
                else:
                    res_dict[item["name"]] += int(item["cost"])
            out_message = "計算したよ！\n" + "\n".join(["{}:{}".format(key, res_dict[key]) for key in res_dict])

    elif commands[0] == "initialize":
        dynamo.batch_delete("gs_t_purchase", "group_id", group_id)
        out_message = "全部消した！！！"

    return out_message

def handle_group_text(event):

    if event["message"]["type"] != "text":
        return

    group_id = event["source"]["groupId"]
    in_message = event["message"]["text"]
    if not in_message:
        return

    dynamo = DynamoDB()
    record = dynamo.get("gs_m_group", {"group_id":group_id})
    if not record:
        record = {"group_id":group_id}
        dynamo.put("gs_m_group", record)
    
    line = Line()
    if is_command(in_message):

        if not is_valid_command(in_message):
            out_message = "分からないコマンドだよ。。@helpで確認してみてね。"
            line = Line()
            line.push(group_id, out_message)
            return
        
        out_message = handle_command(in_message, dynamo, group_id)
        line.push(group_id, out_message)
        return

    docomo = Docomo(dynamo, "group", record)
    out_message = docomo.chat(in_message)
    line.push(group_id, out_message)


def handle_user_text(event):

    if event["message"]["type"] != "text":
        return

    line_mid = event["source"]["userId"]
    in_message = event["message"]["text"]

    if line_mid == "Udeadbeefdeadbeefdeadbeefdeadbeef":
        reply_token = event["replyToken"]
        line = Line()
        line.reply(reply_token, "DeadBeaf")
        return
    
    dynamo = DynamoDB()
    record = dynamo.get("gs_m_user", {"line_mid":line_mid})
    if not record:
        record = {"line_mid":line_mid}
        dynamo.put("gs_m_user", record)

    if in_message:
        docomo = Docomo(dynamo, "user", record)
        out_message = docomo.chat(in_message)
        line = Line()
        line.push(line_mid, out_message)
        return


