#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201031T180000

"""
This script provides global variables, constants, functions in this project
"""

from datetime import datetime
from email import encoders
from email.header import Header, decode_header
from email.parser import Parser
from email.utils import parseaddr, formataddr
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
import os
from poplib import POP3_SSL
import smtplib

import pandas as pd
from pymongo import MongoClient

from WindPy import w

STR_TODAY = datetime.today().strftime('%Y%m%d')
# STR_TODAY = '20210223'


class Globals:
    def __init__(self, str_today=STR_TODAY, download_winddata_mark=0):
        # 日期部分
        self.str_today = str_today
        self.dt_today = datetime.strptime(self.str_today, '%Y%m%d')
        self.server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        self.db_global = self.server_mongodb['global']
        self.col_trdcalendar = self.db_global['trade_calendar']

        self.list_str_trdcalendar = []
        for _ in self.col_trdcalendar.find():
            self.list_str_trdcalendar += _['Data']
        idx_str_today = self.list_str_trdcalendar.index(self.str_today)
        self.str_last_trddate = self.list_str_trdcalendar[idx_str_today - 1]
        self.str_next_trddate = self.list_str_trdcalendar[idx_str_today + 1]
        self.str_last_last_trddate = self.list_str_trdcalendar[idx_str_today - 2]
        self.str_next_next_trddate = self.list_str_trdcalendar[idx_str_today + 2]

        # 配置文件部分: basicinfo
        self.db_basicinfo = self.server_mongodb['basicinfo']
        self.col_acctinfo = self.db_basicinfo['acctinfo']
        self.col_prdinfo = self.db_basicinfo['prdinfo']
        self.col_strategy_info = self.db_basicinfo['strategy_info']

        # pre-trade
        self.fpath_input_csv_excluded_secids = 'data/input/pretrddata/tgtsecids/excluded_secids.csv'
        self.fpath_output_xlsx_provided_secloan_analysis = (
            'data/output/security_loan/provided_security_loan_analysis.xlsx'
        )
        self.db_pretrddata = self.server_mongodb['pre_trade_data']
        self.col_pretrd_grp_tgtsecids_by_cps = self.db_pretrddata['group_target_secids_by_composite']
        self.col_pretrd_secloan_demand_analysis = self.db_pretrddata['secloan_demand_analysis']
        self.col_provided_secloan_analysis = self.db_pretrddata['provided_secloan_analysis']
        self.col_pretrd_rawdata_tgtsecids = self.db_pretrddata['pretrd_rawdata_tgtsecids']
        self.col_pretrd_fmtdata_tgtsecids = self.db_pretrddata['pretrd_fmtdata_tgtsecids']
        self.col_pretrd_rawdata_excluded_secids = self.db_pretrddata['pretrd_rawdata_excluded_secids']
        self.col_pretrd_rawdata_md_private_secloan = self.db_pretrddata['pre_trade_rawdata_md_private_security_loan']
        self.col_pretrd_fmtdata_md_private_secloan = self.db_pretrddata['pre_trade_fmtdata_md_private_security_loan']
        self.col_pretrd_rawdata_md_public_secloan = self.db_pretrddata['pre_trade_rawdata_md_public_security_loan']
        self.col_pretrd_fmtdata_md_security_loan = self.db_pretrddata['pre_trade_fmtdata_md_security_loan']

        self.fpath_output_csv_secloan_demand_analysis = (
            f"data/output/security_loan_demand_analysis/{self.str_today}_security_loan_demand_analysis.csv"
        )
        self.dirpath_output_secloan_order = "data/output/security_loan_order"

        self.col_fmtted_wssdata = self.db_global['fmtted_wssdata']
        df_fmtted_wssdata_today = pd.DataFrame(
            self.col_fmtted_wssdata.find({'DataDate': self.str_today}, {'_id': 0})
        )
        self.dict_fmtted_wssdata_today = df_fmtted_wssdata_today.set_index('WindCode').to_dict()
        df_fmtted_wssdata_last_trddate = pd.DataFrame(
            self.col_fmtted_wssdata.find({'DataDate': self.str_last_trddate}, {'_id': 0})
        )
        self.dict_fmtted_wssdata_last_trddate = df_fmtted_wssdata_last_trddate.set_index('WindCode').to_dict()

        # trade
        self.db_trade_data = self.server_mongodb['trade_data']
        self.col_trade_rawdata_fund = self.db_trade_data['trade_rawdata_fund']
        self.col_trade_rawdata_holding = self.db_trade_data['trade_rawdata_holding']
        self.col_trade_rawdata_order = self.db_trade_data['trade_rawdata_order']
        self.col_trade_rawdata_secloan = self.db_trade_data['trade_rawdata_secloan']
        self.col_trade_secloan_order = self.db_trade_data['trade_secloan_order']
        self.col_trade_fmtdata_public_secloan = self.db_trade_data['trade_fmtdata_public_security_loan']
        self.col_trade_fmtdata_private_secloan = self.db_trade_data['trade_fmtdata_private_security_loan']
        self.col_trade_ssquota_from_secloan = self.db_trade_data['trade_ssquota_from_security_loan']

        # post-trade
        self.db_posttrddata = self.server_mongodb['post_trade_data']

        self.col_posttrd_rawdata_fund = self.db_posttrddata['post_trade_raw_data_fund']
        self.col_posttrd_fmtdata_fund = self.db_posttrddata['post_trade_formatted_data_fund']
        self.col_posttrd_rawdata_holding = self.db_posttrddata['post_trade_raw_data_holding']
        self.col_posttrd_fmtdata_holding = self.db_posttrddata['post_trade_formatted_data_holding']

        self.col_posttrd_rawdata_short_position = self.db_posttrddata['post_trade_raw_data_short_position']
        self.col_posttrd_fmtdata_short_position = self.db_posttrddata['post_trade_formatted_data_short_position']
        self.col_posttrd_rawdata_public_secloan = self.db_posttrddata['post_trade_raw_data_public_security_loan']
        self.col_posttrd_fmtdata_public_secloan = self.db_posttrddata['post_trade_formatted_data_public_security_loan']
        self.col_posttrd_rawdata_private_secloan = self.db_posttrddata['post_trade_raw_data_private_security_loan']
        self.col_posttrd_fmtdata_private_secloan = (
            self.db_posttrddata['post_trade_formatted_data_private_security_loan']
        )
        self.col_posttrd_rawdata_jgd = self.db_posttrddata['post_trade_raw_data_jgd']
        self.col_posttrd_fmtdata_jgd = self.db_posttrddata['post_trade_formatted_data_jgd']
        self.col_posttrd_rawdata_fee_from_secloan = self.db_posttrddata['post_trade_raw_data_fee_from_security_loan']
        self.col_posttrd_fmtdata_fee_from_secloan = (
            self.db_posttrddata['post_trade_formatted_data_fee_from_security_loan']
        )

        self.col_posttrd_position = self.db_posttrddata['post_trade_position']
        self.col_posttrd_fmtdata_ssquota_from_secloan = (
            self.db_posttrddata['post_trade_formatted_data_short_selling_quota_from_security_loan']
        )

        self.col_posttrd_pnl = self.db_posttrddata['post_trade_pnl']
        self.col_posttrd_pnl_by_secid = self.db_posttrddata['post_trade_pnl_by_security_id']
        self.col_posttrd_pnl_by_acctidbymxz_cps = self.db_posttrddata['post_trade_pnl_by_acctidbymxz_cps']
        self.col_posttrd_secloan_utility_analysis = self.db_posttrddata['post_trade_security_loan_utility_analysis']
        self.col_posttrd_pnl_by_acctidbymxz = self.db_posttrddata['post_trade_pnl_by_acctidbymxz']
        self.col_posttrd_cf_from_indirect_method = self.db_posttrddata['post_trade_cash_flow_from_indirect_method']
        self.col_posttrd_fmtdata_excluded_secids = (
            self.db_posttrddata['post_trade_formatted_data_excluded_security_ids']
        )

        self.fpath_output_xlsx_posttrd_analysis = 'D:/projects/autot0/data/output/posttrd_analysis.xlsx'

        # 其他
        self.dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE'}

    @classmethod
    def get_secid2windcode(cls, str_secid):
        # 将沪深交易所标的代码转为windcode
        if len(str_secid) == 6:
            if str_secid[0] in ['0', '3']:
                __windcode = str_secid + '.SZ'
                return __windcode
            elif str_secid[0] in ['6']:
                __windcode = str_secid + '.SH'
                return __windcode
            elif str_secid[0] in ['5']:
                __windcode = str_secid + '.SH'
                return __windcode
            else:
                raise ValueError('Wrong secid first letter: Cannot get windcode suffix according to SecurityID.')
        else:
            raise ValueError('The length of the security ID is not 6.')

    @staticmethod
    def decode_str(__str):
        value, charset = decode_header(__str)[0]
        if charset:
            value = value.decode(charset)
        return value

    @staticmethod
    def fmt_email_addr(__str):
        name, addr = parseaddr(__str)
        ret = formataddr((Header(name, 'utf-8').encode(), addr))
        return ret

    def get_attachment(self, msg, dirpath_output_attachment, date_in_fn=1):
        for part in msg.walk():
            # 获取附件名称类型
            file_name = part.get_filename()
            if file_name:
                h = Header(file_name)
                # 对附件名称进行解码
                dh = decode_header(h)
                fn = dh[0][0]
                if dh[0][1]:
                    # 将附件名称可读化
                    if date_in_fn:
                        fn = self.decode_str(str(fn, dh[0][1]))
                    else:
                        fn = self.decode_str(str(fn, dh[0][1])).replace('-', '').replace(self.str_today, '')

                # 下载附件
                data = part.get_payload(decode=True)
                # 在指定目录下创建文件，注意二进制文件需要用wb模式打开
                fpath_output_attachment = os.path.join(dirpath_output_attachment, fn)
                if not os.path.exists(dirpath_output_attachment):
                    os.mkdir(dirpath_output_attachment)
                with open(fpath_output_attachment, 'wb') as f:
                    f.write(data)  # 保存附件
                    print(f"The attachment {fn} has been downloaded to {dirpath_output_attachment}.")

    def update_attachments_from_email(self, str_email_subject, str_email_tgtdate, dirpath_output_attachment, date_in_fn):
        # 下载指定邮件中的所有附件到指定文件夹中(文件名、格式均不变)，如果文件夹不存在，则新建文件夹
        # 从邮件中下载数据
        addr_pop3_server = 'pop.exmail.qq.com'
        server_pop3 = POP3_SSL(addr_pop3_server, 995)
        server_pop3.user(self.email_from_addr)
        server_pop3.pass_(self.email_pwd)
        resp, mails, octets = server_pop3.list()
        index = len(mails)  # 若有30封邮件，第30封邮件为最新的邮件
        mark_tgtmail_exist = 0
        for i in range(index, 0, -1):  # 倒叙遍历邮件
            resp, lines, octets = server_pop3.retr(i)
            msg_content = b'\r\n'.join(lines).decode('utf-8')
            msg = Parser().parsestr(msg_content)
            subject = self.decode_str(msg.get('Subject')).strip()
            str_date_in_msg = self.decode_str(msg.get('date')).strip()
            list_str_date_in_msg_split = str_date_in_msg.split()
            str_date_in_msg = ' '.join(list_str_date_in_msg_split[:4])
            try:
                dt_email_recvdate = datetime.strptime(str_date_in_msg, '%a, %d %b %Y')
            except ValueError:
                dt_email_recvdate = datetime.strptime(str_date_in_msg.split()[0], '%Y-%m-%d')

            str_email_recvdate = dt_email_recvdate.strftime('%Y%m%d')
            if subject == str_email_subject and str_email_recvdate == str_email_tgtdate:
                self.get_attachment(msg, dirpath_output_attachment, date_in_fn)
                mark_tgtmail_exist = 1
                break
        if not mark_tgtmail_exist:
            print(f'Warning: {str_email_subject} not found.')
        server_pop3.quit()
        print('The attachment from email download finished.')

    def send_file_via_email(self, email_to_addr, subject, fpath_file, fn_attachment):
        # 通过邮件发送文件
        # 发送的文件为单一附件
        addr_smtp_server = 'smtp.exmail.qq.com'
        msg = MIMEMultipart()
        msg['From'] = self.fmt_email_addr(f'马新哲<{self.email_from_addr}>')
        msg['To'] = self.fmt_email_addr(f'<{email_to_addr}>')
        msg['Subject'] = Header(subject, 'utf-8').encode()
        with open(fpath_file, 'rb') as f:
            mime = MIMEBase('application', 'octet-stream')
            mime.set_payload(f.read())
            encoders.encode_base64(mime)
            mime.add_header('Content-Disposition', 'attachment', filename=fn_attachment)
            msg.attach(mime)
        server_smtp = smtplib.SMTP(addr_smtp_server)
        server_smtp.starttls()
        server_smtp.login(self.email_from_addr, self.email_pwd)
        server_smtp.sendmail(self.email_from_addr, email_to_addr, msg.as_string())
        print('The mail has been sent.')
        server_smtp.quit()

    @staticmethod
    def get_list_str_trddate(str_startdate, str_enddate):
        """
        获得期间自然日期间的交易日，参数为闭区间两端
        :param str_startdate: 起始日期，包括
        :param str_enddate: 终止日期，包括
        :return: list, 指定期间内交易日的列表
        """
        wtdays = w.tdays(str_startdate, str_enddate)
        list_str_trddates = [x.strftime('%Y%m%d') for x in wtdays.Data[0]]
        return list_str_trddates

    @classmethod
    def get_mingshi_sectype_from_code(cls, str_code):
        """
        实际使用的函数
        :param str_code: SecurityID.SecurityIDSource
            1. SecuritySource:
                1. SSE: 上交所
                2. SZSE: 深交所
                3. ITN: internal id
        :return:
            1. CE, Cash Equivalent, 货基，质押式国债逆回购
            2. CS, Common Stock, 普通股
            3. ETF, ETF, 注意：货币类ETF归类为CE，而不是ETF
            4. SWAP, swap
        """

        list_split_wcode = str_code.split('.')
        secid = list_split_wcode[0]
        exchange = list_split_wcode[1]
        if exchange in ['SH', 'SSE'] and len(secid) == 6:
            if secid in ['511990', '511830', '511880', '511850', '511660', '511810', '511690']:
                return 'CE'
            elif secid in ['204001']:
                return 'CE'
            elif secid[:3] in ['600', '601', '603', '688']:
                return 'CS'
            elif secid in ['510500', '000905', '512500']:
                return 'ETF'
            else:
                return 'IrrelevantItem'

        elif exchange in ['SZ', 'SZSE'] and len(secid) == 6:
            if secid[:3] in ['000', '001', '002', '003', '004', '300', '301', '302', '303', '304', '305', '306', '307',
                             '308', '309']:
                return 'CS'
            elif secid[:3] in ['115', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129']:
                return '可转债'
            elif secid[:3] in ['131']:
                return 'CE'
            elif secid in ['159001', '159005', '159003']:
                return 'CE'
            else:
                return 'IrrelevantItem'
        elif exchange in ['CFE', 'CFFEX']:
            return 'Index Future'

        elif exchange == 'ITN':
            sectype = secid.split('_')[0]
            return sectype

        else:
            raise ValueError(f'{str_code} has unknown exchange or digit number is not 6.')

    @staticmethod
    def secloan_match(fpath_input_csv_target_secids, fpath_xlsx_secloan_secids_2b_matched):
        """
        两个券池进行匹配，求交集
        """
        df_secloan_secids_tgt = pd.read_csv(
            fpath_input_csv_target_secids,
            converters={'SecurityID': lambda x: str(x).zfill(6)}
        )
        list_secloan_secids_tgt = df_secloan_secids_tgt['SecurityID'].to_list()
        set_secloan_secids_tgt = set(list_secloan_secids_tgt)
        if os.path.exists(fpath_xlsx_secloan_secids_2b_matched):
            dict_dfs_secloan_secids_2b_matched = pd.read_excel(
                fpath_xlsx_secloan_secids_2b_matched,
                sheet_name=None,
                converters={
                    'code': lambda x: str(x).strip().zfill(6),
                    '证券代码': lambda x: str(x).strip().zfill(6),
                    'stkId': lambda x: str(x).strip().zfill(6),
                }
            )

            list_secid_fields_name = ['SecurityID', 'code', '证券代码', 'stkId']  # 标题栏
            set_secloan_secids_2b_matched = set()
            for sheet_name, df_ in dict_dfs_secloan_secids_2b_matched.items():
                for secid_field_name in list_secid_fields_name:
                    if secid_field_name in df_.columns:
                        for secid in df_[secid_field_name].to_list():
                            set_secloan_secids_2b_matched.add(secid)

            set_matched_secloan_secids = set_secloan_secids_2b_matched & set_secloan_secids_tgt
            list_matched_secloan_secids = list(set_matched_secloan_secids)
            list_matched_secloan_secids.sort()
            i_secloan_secids = len(list_matched_secloan_secids)
            print(i_secloan_secids, fpath_xlsx_secloan_secids_2b_matched, list_matched_secloan_secids)
        else:
            i_secloan_secids = 'DataFileNotExists'
            set_matched_secloan_secids = set()
        return i_secloan_secids, set_matched_secloan_secids

    # def output_provided_secloan_analysis_xlsx(self):
    #     list_broker_alias_for_secloan_analysis = ['hait', 'huat', 'swhy', 'gtja', 'zx']
    #     list_dicts_secloan_analysis_among_brokers = []
    #     set_matched_secloan_secids = set()
    #     for broker_alias in list_broker_alias_for_secloan_analysis:
    #         i_secloan_secids, set_matched_secloan_secids_delta = task.secloan_match(
    #             f'D:/projects/autot0/data/input/pretrddata/tgtsecids/{self.str_today}_target_secids.csv',
    #             f'D:/projects/autot0/data/input/pretrddata/market_data/{broker_alias}/'
    #             f'每日券池-{self.str_today}.xlsx'
    #         )
    #         set_matched_secloan_secids = set_matched_secloan_secids | set_matched_secloan_secids_delta
    #         dict_secloan_analysis_among_brokers = {
    #             'DataDate': self.str_today,
    #             'BrokerAlias': broker_alias,
    #             'IntersectionSecurityCount': i_secloan_secids
    #         }
    #         list_dicts_secloan_analysis_among_brokers.append(dict_secloan_analysis_among_brokers)
    #     self.col_provided_secloan_analysis.delete_many({'DataDate': self.str_today})
    #     if list_dicts_secloan_analysis_among_brokers:
    #         self.col_provided_secloan_analysis.insert_many(list_dicts_secloan_analysis_among_brokers)
    #     # print(len(set_matched_secloan_secids), set_matched_secloan_secids)
    #     iter_col_trade_secloan = self.db_trade_data['trade_ssquota_from_security_loan']
    #     set_secloan_secids_in_ssquota = set()
    #     for secid in iter_col_trade_secloan.find({'DataDate': self.str_today}):
    #         set_secloan_secids_in_ssquota.add(secid['SecurityID'])
    #
    #     # print(len(set_secloan_secids_in_ssquota))
    #
    #     set_secloan_secids_tgt = set_matched_secloan_secids | set_secloan_secids_in_ssquota
    #     # print(len(set_secloan_secids_tgt), set_secloan_secids_tgt)
    #
    #
    #     iter_secloan_analysis = self.col_provided_secloan_analysis.find({}, {'_id': 0})
    #     df_secloan_analysis = pd.DataFrame(iter_secloan_analysis)
    #     df_secloan_analysis.to_excel(self.fpath_output_xlsx_provided_secloan_analysis, index=False)
    #
    #     print('Output provided_security_loan_analysis.xlsx Finished.')


if __name__ == '__main__':
    # 盘后运行, 更新数据库
    # task = Globals('20210202', download_winddata_mark=1)
    # task.output_provided_secloan_analysis_xlsx()
    print('Done')
