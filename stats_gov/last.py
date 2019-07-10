import functools, os, time, aiohttp, asyncio, random, re, csv, urllib.parse
import datetime as dt
from bs4 import BeautifulSoup
import logging

DEBUG = False

HTML_ENCODEING = 'gb18030'
BASE_PATH = '../China_Province_2018/'
if not os.path.isdir(BASE_PATH):
    os.mkdir(BASE_PATH)


failed_urls = []


ua_list = [
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50"
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv,2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Mozilla/5.0 (Windows NT 6.1; rv,2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11"
]

Headers = {
'Host': 'www.stats.gov.cn',
'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:67.0) Gecko/20100101 Firefox/67.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'en-US,en;q=0.5',
'Accept-Encoding': 'gzip, deflate',
#'Referer': 'http://www.stats.gov.cn/waf_verify.htm',
'Connection': 'close',
'Cookie': 'wzws_cid=73c7a000f433fb74168beff76534b616002ea551315bc0904592d14b0eb6c89efc7a6d17fa6ee82f5f772de3c14fbf641d38f786ce7ac19c3afb3082f0c85ce4; AD_RS_COOKIE=20082855',
'Upgrade-Insecure-Requests': '1',
'If-Modified-Since': 'Fri, 22 Feb 2019 08:27:52 GMT',
'If-None-Match': "1044-582775debb200-gzip",
'Cache-Control': 'max-age=0',
}

def log_init():
    global logger

    logger = logging.getLogger('last')
    logger.setLevel('DEBUG')
    BASIC_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s [:%(lineno)d] - %(message)s"
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)

    chlr = logging.StreamHandler()
    chlr.setFormatter(formatter)
    chlr.setLevel('DEBUG')

    fhlr = logging.FileHandler('last.log')
    fhlr.setFormatter(formatter)
    fhlr.setLevel('INFO')

    logger.addHandler(chlr)
    logger.addHandler(fhlr)


async def get_html(sem, url, handle, result, counter=None):
    global failed_urls

    if not isinstance(result, list):
        raise TypeError('result must be a list')
    if not isinstance(url, str):
        raise TypeError('url must be a string')
    if not isinstance(counter, dict):
        counter = {'all': 0, 'done': -1, 'now': dt.datetime.now()}
    async with sem:
        async with aiohttp.ClientSession() as session:
            while True:
                status = -1
                try:
                    async with session.get(url, headers=Headers, timeout=20) as resp:
                        if resp.status != 200:
                            status = resp.status
                            raise Exception(str(status))
                        else:
                            # response = await resp.content.read()
                            try:
                                response = await resp.text(HTML_ENCODEING)
                            except TimeoutError as e:
                                raise e
                            except aiohttp.ClientPayloadError as e:
                                raise e
                            except (UnicodeDecodeError, UnicodeEncodeError) as e:
                                print(url, e)
                                failed_urls.append(url)
                                break
                                # exit(0)
                            except Exception as e:
                                print('@' * 100)
                                raise e
                            else:
                                result.extend(handle(response, url))
                                counter['done'] += 1
                                dt.timedelta().total_seconds()
                                print('{} \tsuccess! \t({}/{}) \t{}'.format(url, counter['done'], counter['all'],
                                                                            str(dt.datetime.now() - counter['now'])[:-7]))
                                await asyncio.sleep(1)
                                break
                except Exception as e:
                    await asyncio.sleep(3 + random.random() * 7)
                    logger.info('{} \tretry due to status:{}\t{}'.format(url, status, repr(e)))


def get_htmls_and_handle(url_list, handle, count=False):
    sem = asyncio.Semaphore(1)
    tasks = []
    result = []
    counter = {'all': len(url_list), 'done': 0, 'now': dt.datetime.now()} if count else None
    for url in url_list:
        tasks.append(get_html(sem, url, handle, result, counter))
    if not tasks:
        return None
        # raise ValueError('tasks is empty')
    loop = asyncio.get_event_loop()
    start_time = dt.datetime.now()
    loop.run_until_complete(asyncio.wait(tasks))
    logger.info('#' * 100)
    logger.info('time cost :{}, \t({}) tasks all done!'.format(str(dt.datetime.now() - start_time)[:-7], len(tasks)))
    logger.info('#' * 100)
    return result


def current_level(soup):
    if soup.select('table.citytable > tr.citytr'):
        return 2
    elif soup.select('table.countytable > tr.countytr'):
        return 3
    elif soup.select('table.towntable > tr.towntr'):
        return 4
    else:
        raise Exception('can not recognition current level')


def fun2(response, req_url):
    # pattern = re.compile("<a href='(.*?)'>(.*?)<")
    # result = list(set(re.findall(pattern, response)))
    soup = BeautifulSoup(response, 'lxml')
    a_list = soup.select('table > tr > td:nth-of-type(2) > a')

    tr_list = soup.select('table.villagetable > tr.villagetr')
    result = [[i('td')[0].text, i('td')[1].text, i('td')[2].text] for i in tr_list]
    return [ [code1,
                            code2,
                            address,
                            '1' if code2[0] == '1' else '0',
                            ] for code1, code2, address in result]


def main(file):
    result = []
    url_list = []

    with open(file, 'r') as f:
        line = f.readline()

    line = line.split(',')
    logger.info(line)

    for i in line:
        url_list.append(i)
    # handle_kwargs_list.append(row['kwargs'])
    temp = get_htmls_and_handle(url_list, fun2, count=True)

    if temp:
        result.extend(temp)
        result.sort()

        with open(os.path.join(BASE_PATH, 'csv_{}.csv'.format(dt.datetime.now().strftime("%Y%m%d%H%M"))), 'w',
                  newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in result:
                writer.writerow(row)


if __name__ == '__main__':
    log_init()

    file = 'failed_urls_201907091344.txt'
    main(file)

    failed_file = 'failed_urls_{}.txt'.format(dt.datetime.now().strftime("%Y%m%d%H%M"))

    with open(failed_file, 'w+') as f:
        for url in failed_urls:
            f.write(url)
            f.write('\n')
