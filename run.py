import requests
import time
import hashlib
import hmac
from urllib.parse import urlencode
import pandas as pd
from datetime import datetime, timezone
import tarfile
import os
import re
import json
from pprint import pprint as pp
import sys

class PrepareData:
    def __init__(self):
        pass

    def run(self, filename):
        # Распаковываем месячный архив
        foldername = filename.split(".")[0]
        listfiles = list()

        with tarfile.open(f"./{filename}") as to:
            listfiles = to.getnames()
            to.extractall(foldername)

        for n in listfiles:
            with tarfile.open(f"./{foldername}/{n}") as to:
                to.extractall(f"./{foldername}")
            os.remove(f"./{foldername}/{n}")
            os.remove(f"./{foldername}/{n.split('.')[0]}_depth_snap.csv")
        
        # Открываем файлы из директории
        for n in os.listdir(f"./{foldername}"):
            # Скипаем, если наш клиент (Необходимо, если запускаем повторно)
            if 'depth_update' not in n:
                continue
            
            # Читаем файл
            df = pd.read_csv(f"./{foldername}/{n}")
            # Удаляем ненужные колонки
            # Некоторые файлы без header, на этот случай будем отлавливать такие ошибки и явно задавать header
            try:
                df.drop(columns=['symbol','first_update_id','last_update_id','update_type', 'pu'], axis=1, inplace=True)
            except KeyError:
                df.set_axis(['symbol', 'timestamp', 'first_update_id', 'last_update_id', 'side', 'update_type', 'price', 'qty', 'pu'], axis=1, inplace=True)
                df.drop(columns=['symbol','first_update_id','last_update_id','update_type', 'pu'], axis=1, inplace=True)
            # Удаляем дубликаты
            df.drop_duplicates(subset=['timestamp', 'side', 'price', 'qty'], keep='first', inplace=True)
            # Рассчитываем задержку между отправками данных
            d = df.copy()
            d.drop(columns=['side','price', 'qty'], axis=1, inplace=True)
            d.drop_duplicates(inplace=True)
            # Функция расчета дельты
            d = d.assign(delay=lambda x: (d['timestamp'].shift(-1) - d['timestamp']))
            # Заполняем вычисленной задержкой основной массив данных
            df['delay'] = df['timestamp'].map(d.set_index('timestamp')['delay'])
            df['delay'] = df['delay'].fillna(100)
            # Сохраняем обработанные данные
            newname = (re.findall("([\d]{4}-[\d]{2}-[\d]{2})", n)[0]).replace("-","")+'.csv'
            df.to_csv( f"./{foldername}/{newname}", index=False)
            os.remove(f"./{foldername}/{n}")

        return True

class GetData:
    S_URL_V1 = None
    api_key = None
    secret_key = None

    symbol = None
    startTime = None
    endTime = None
    config = None
    loads = list()
    pdata = None

    def __init__(self):
        try:
            with open("./config.json", "r") as fp:
                self.config = json.loads(fp.read())
            
            self.S_URL_V1 = self.config['url']
            self.api_key = self.config['apikey']
            self.secret_key = self.config['secretkey']
            self.loads = self.config['loads']
            self.pdata = PrepareData()
        except Exception as e:
            print(e)

    def getAll(self):
        def _statusNew():
            print("### Запрос на постановку в очередь")
            paramsToObtainDownloadID = {
                    "symbol": self.loads[n]['symbol'],
                    "startTime": int(datetime.strptime(self.loads[n]['start'], "%d.%m.%Y %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()*1000),
                    "endTime": int(datetime.strptime(self.loads[n]['end'], "%d.%m.%Y %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()*1000),
                    "dataType": "T_DEPTH",
                    "timestamp": str(int(1000 * time.time()))
                }
            path = "%s/futuresHistDataId" % self.S_URL_V1
            download_response = self.post(path, paramsToObtainDownloadID)
            try:
                downloadID = download_response.json()["id"]
                if downloadID:
                    print("Получен ID: " + str(downloadID))
                    self.loads[n]['id'] = int(downloadID)
                    self.loads[n]['status'] = "waitLink"
                    self.saveStatus()
                _statusWaitLink()
            except:
                print(download_response.text)
    
        def _statusWaitLink():
            print("### Запрос на получение сформированного архива")
            paramsToObtainDownloadID = {
                "symbol": self.loads[n]['symbol'],
                "startTime": int(datetime.strptime(self.loads[n]['start'], "%d.%m.%Y %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()*1000),
                "endTime": int(datetime.strptime(self.loads[n]['end'], "%d.%m.%Y %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()*1000),
                "dataType": "T_DEPTH",
                "timestamp": str(int(1000 * time.time()))
            }

            paramsToObtainDownloadLink = {"downloadId": self.loads[n]['id'], "timestamp": str(int(1000 * time.time()))}
            pathToObtainDownloadLink = "%s/downloadLink" % self.S_URL_V1
            resultToBeDownloaded = self.get(pathToObtainDownloadLink, paramsToObtainDownloadLink)
            link = resultToBeDownloaded.json()['link']

            if link and 'Link is preparing' not in link:
                print("Сформирована ссылка на скачивание: " + link)
                self.loads[n]['link'] = link
                self.loads[n]['status'] = "download"
                self.saveStatus()
                _statusDownload()

        def _statusDownload():
            print("### Скачивание файла")
            file_name = f"{self.loads[n]['symbol']}{self.loads[n]['id']}.tar.gz"
            with open(file_name, "wb") as f:
                print("Downloading %s" % file_name)
                response = requests.get(self.loads[n]['link'], stream=True)
                total_length = response.headers.get('content-length')

                if total_length is None:
                    f.write(response.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in response.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s] total size: %.2f Gb" % ('=' * done, ' ' * (50-done),(total_length/1000000000)) )    
                        sys.stdout.flush()
                    self.loads[n]['status'] = "prepareData"
                    self.saveStatus()
                    _statusPrepareData()

        def _statusPrepareData():
            print("### Обрабатываем файл")
            filename = f"{self.loads[n]['symbol']}{self.loads[n]['id']}.tar.gz"
            if self.pdata.run(filename):
                self.loads[n]['status'] = "finish"
                self.saveStatus()
                os.remove(f"./{filename}")
            
        for n in range(0, len(self.loads)):
            if self.loads[n]['status'] == 'new':
                _statusNew()
            elif self.loads[n]['status'] == 'waitLink':
                _statusWaitLink()
            elif self.loads[n]['status'] == 'download':
                _statusDownload()
            elif self.loads[n]['status'] == 'prepareData':
                _statusPrepareData()

    def saveStatus(self):
        self.config['loads'] = self.loads
        with open("./config.json", "w") as fp:
            fp.write(json.dumps(self.config, indent=4))

    def _sign(self, params={}):
        data = params.copy()
        ts = str(int(1000 * time.time()))
        data.update({"timestamp": ts})
        h = urlencode(data)
        h = h.replace("%40", "@")

        b = bytearray()
        b.extend(self.secret_key.encode())

        signature = hmac.new(b, msg=h.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
        sig = {"signature": signature}

        return data, sig

    def post(self, path, params={}):
        sign = self._sign(params)
        query = urlencode(sign[0]) + "&" + urlencode(sign[1])
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.api_key}
        resultPostFunction = requests.post(url, headers=header, timeout=30, verify=True)
        return resultPostFunction

    def get(self, path, params):
        sign = self._sign(params)
        query = urlencode(sign[0]) + "&" + urlencode(sign[1])
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.api_key}
        resultGetFunction = requests.get(url, headers=header, timeout=30, verify=True)
        return resultGetFunction

getData = GetData()
getData.getAll()