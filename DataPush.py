import logging
import time
import pymysql
import schedule
from DBUtils.PooledDB import PooledDB


class MysqlUpdate(object):
    __local_pool = None
    __cloud_pool = None

    def __init__(self):
        """初始化配置"""

        # 本地数据库
        self.local_host = '10.143.100.55'
        self.local_port = 3306
        self.local_user = 'june'
        self.local_passwd = 'june123'
        self.local_db = 'baidusem'
        self.local_table = 'baidu_sem'
        self.local_conn = self.get_local_conn()
        self.local_cur = self.local_conn.cursor(cursor=pymysql.cursors.DictCursor)

        # 云数据库
        self.cloud_host = '40.73.39.227'
        self.cloud_port = 443
        self.cloud_user = 'june'
        self.cloud_passwd = 'june123'
        self.cloud_db = 'baiduapi'
        self.cloud_table = 'baidu_sem'
        self.cloud_conn = self.get_cloud_conn()
        self.cloud_cur = self.cloud_conn.cursor(cursor=pymysql.cursors.DictCursor)

        # 日志
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.NOTSET)
        handler = logging.FileHandler("log.txt")
        handler.setLevel(logging.NOTSET)
        formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        console = logging.StreamHandler()
        console.setLevel(logging.NOTSET)
        self.logger.addHandler(handler)
        self.logger.addHandler(console)

    def get_local_conn(self):
        """本地数据库连接池连接"""

        if MysqlUpdate.__local_pool is None:
            MysqlUpdate.__local_pool = PooledDB(creator=pymysql, mincached=1, maxcached=20, host=self.local_host,
                                                user=self.local_user, passwd=self.local_passwd, db=self.local_db,
                                                port=self.local_port, charset='utf8')
        conn = MysqlUpdate.__local_pool.connection()
        return conn

    def get_cloud_conn(self):
        """云数据库连接池连接"""

        if MysqlUpdate.__cloud_pool is None:
            MysqlUpdate.__cloud_pool = PooledDB(creator=pymysql, mincached=1, maxcached=3, host=self.cloud_host,
                                                user=self.cloud_user, passwd=self.cloud_passwd, db=self.cloud_db,
                                                port=self.cloud_port, charset='utf8')
        coon = MysqlUpdate.__cloud_pool.connection()
        return coon

    def run(self):
        """执行程序"""

        try:
            # 查出本地数据库最大ID
            self.local_cur.execute('select max(ID) from %s;' % self.local_table)
            max_local = self.local_cur.fetchone()['max(ID)']

            # 查出云数据库最大ID
            self.cloud_cur.execute('select max(ID) from %s;' % self.cloud_table)
            max_cloud = self.cloud_cur.fetchone()['max(ID)']

            # 如果本地ID大于云ID则说明数据有更新，执行更新
            if max_local > max_cloud:
                for i in range(max_cloud + 1, max_local + 1):
                    # 先查出本地数据
                    sql = 'select * from %s where ID=%s;' % (self.local_table, i)
                    self.local_cur.execute(sql)
                    data = self.local_cur.fetchone()
                    # 如果有数据，修改一下数据格式（主要是时间类型转为字符串），并构造sql语句
                    if data:
                        key_list = []
                        value_list = []
                        for key, value in data.items():
                            key_list.append(key)
                            if str(type(value)) == "<class 'datetime.date'>":
                                value_list.append("'" + value.strftime('%Y-%m-%d') + "'")
                            elif str(type(value)) == "<class 'datetime.datetime'>":
                                value_list.append(value.strftime("'" + '%Y-%m-%d %H:%M:%S') + "'")
                            elif str(type(value)) == "<class 'int'>":
                                value_list.append(str(value))
                            elif value is None:
                                value_list.append('')
                            else:
                                value_list.append("'" + str(value).replace("'", '"') + "'")
                        sql = 'insert into %s(%s) values(%s);' % (
                            self.cloud_table, ','.join(key_list), ','.join(value_list))
                        # 插入到云数据库
                        self.cloud_cur.execute(sql)
                        self.cloud_conn.commit()
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            self.logger.error("Something wrong:", exc_info=True)


if __name__ == '__main__':
    mysql_update = MysqlUpdate()

    # 定时任务
    schedule.every().day.at("00:00").do(mysql_update.run)
    while True:
        schedule.run_pending()
        time.sleep(1)
