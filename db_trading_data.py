#!usr/bin/env python37
# coding:utf-8
# Author: Maxincer
# Create Datetime: 20200413T112000 +0800
# Update Datetime: 20200503T170000 +0800

"""
Update:
Apply to protocol FIX 5.0 sp2

出于开发速度及借鉴国际做法，暂将持仓数据分为 capital 与 holding 两类表

DataBase "trddata" maintenance.

Maintained DataBase:
    1. DB: trddata
        1. col: 各账户structured data
        2. col: 上传的原始数据
        3. All trading data is in the database.

Naming Convention:
    1. 代码中的命名，遵循缩写可作为形容词，与其他名称连拼的规则；
    2. 数据库的命名，出于阅读便利考虑，不可缩写，不可连拼，严格遵循蛇底式拼读法。
    3. the collection should be named as f"{prdcode}_{accttype}_{acctid}_{content}}
        1. content include:
            1. b/s_sheet
            2. holding
            3. trade
            4. transfer serial
    4. 融券负债：Securities Liability。 Debt 从属于liability 且多为现金义务。

Input:
    0. basic_info.xlsx
    1. downloaded data from trading software.
    2. manually patched data.

Process:
    循环遍历account basic info, 进行如下处理：
        1. raw data upload.
            1. read and upload downloaded data.
                1. read general format: capital + security holding
                2. read specific format: distinguished by broker and trading software.
            2. read and upload patched data.
            3. download and upload api data.

        2. raw data to structured data.
            1. structure cash account data.
            2. structure margin account data.
            3. structure future account data.

Note:
    1. AcctType: cash, margin, future
    2. ColType: capital, holding

Abbr.:
    1. ss: short selling
    2. dt: datetime


"""

import codecs
import os

from openpyxl import load_workbook
import pandas as pd
import pymongo
from WindPy import *
from xlrd import open_workbook

from trader_v1 import Trader


class DBTradingData:
    def __init__(self):
        self.dt_today = datetime.today()
        # self.dt_today = datetime(2020, 10, 23)
        self.str_today = datetime.strftime(self.dt_today, '%Y%m%d')
        w.start()
        self.str_last_trddate = w.tdaysoffset(-1, self.str_today, "").Data[0][0].strftime('%Y%m%d')
        self.df_mktdata_from_wind = self.get_close_from_wind()
        self.client_mongo = pymongo.MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        self.db_trddata = self.client_mongo['trddata']
        self.db_basicinfo = self.client_mongo['basicinfo']
        self.col_acctinfo = self.db_basicinfo['acctinfo']
        self.col_prdinfo = self.db_basicinfo['prdinfo']
        self.dirpath_data_from_trdclient = 'data/trdrec_from_trdclient'
        self.fpath_datapatch_relative = 'data/data_patch.xlsx'
        self.dict_future2multiplier = {'IC': 200, 'IH': 300, 'IF': 300}
        self.col_fmtted_dwitems = self.db_trddata['fmtted_dwitems']
        self.col_manually_patchdata_dwitems = self.db_trddata['manually_patchdata_dwitems']
        self.col_na_allocation = self.db_trddata['na_allocation']
        self.col_bs_by_prdcode = self.db_trddata['b/s_by_prdcode']
        self.col_tgtna_by_prdcode = self.db_trddata['tgtna_by_prdcode']
        self.col_formatted_holding = self.db_trddata['formatted_holding']
        self.col_cpslongamt_from_sse_by_acctidbymxz = self.db_trddata['col_cpslongamt_from_sse_by_acctidbymxz']
        self.col_cpslongamt_from_sse_by_prdcode = self.db_trddata['col_cpsamt_from_sse_by_prdcode']

    @staticmethod
    def get_recdict_from_two_adjacent_lines(list_datalines, i_row, encoding='utf-8'):
        """
        :param list_datalines: consists of bytes
        :param i_row: position of the columns
        :param encoding:
        :return: recdict
        """
        dict_rec = {}
        list_keys = [data.decode(encoding) for data in list_datalines[i_row].strip().split(b',')]
        if list_datalines[i_row] and list_datalines[i_row + 1]:
            list_values = [data.decode(encoding) for data in list_datalines[i_row + 1].strip().split(b',')]
            dict_rec.update(dict(zip(list_keys, list_values)))
        return dict_rec

    def read_rawdata_from_trdclient(self, fpath, str_c_h_secliability_mark, data_source_type,
                                    accttype, acctidbybroker, dlddata_filter):
        """
        从客户端下载数据，并进行初步清洗。为字符串格式。
        tdx倒出的txt文件有“五粮液错误”，使用xls格式的可解决

        已更新券商处理格式：
            华泰: hexin, txt, cash, margin, capital, holding
            国君: 富易, csv
            海通: ehtc, xlsx, cash, capital, holding
            申宏: alphabee, txt
            建投: alphabee, txt
            中信: tdx, txt, vip, cash, capital, holding,
            民生: tdx, txt
            华福: tdx, txt

        :param acctidbybroker: 用于pb类文件对账户编号的过滤。
        :param fpath:
        :param accttype: c: cash, m: margin, f: future
        :param str_c_h_secliability_mark: ['capital', 'holding', 'secliability']
        :param data_source_type:

        :return: list: 由dict rec组成的list
        """
        list_ret = []
        if str_c_h_secliability_mark == 'capital':
            dict_rec_capital = {}
            if data_source_type in ['huat_hx', 'hait_hx', 'zhes_hx', 'tf_hx', 'db_hx', 'wk_hx'] and accttype == 'c':
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()[0:6]
                    for dataline in list_datalines:
                        list_data = dataline.strip().split(b'\t')
                        for data in list_data:
                            list_recdata = data.strip().decode('gbk').split('：')
                            dict_rec_capital[list_recdata[0].strip()] = list_recdata[1].strip()

            elif data_source_type in ['yh_hx'] and accttype in ['c']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[5].decode('gbk').split()
                    list_values = list_datalines[6].decode('gbk').split()
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['yh_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_capital = df_read.to_dict('records')[0]

            elif data_source_type in ['huat_hx', 'hait_hx', 'wk_hx'] and accttype == 'm':
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()[5:14]
                    for dataline in list_datalines:
                        if dataline.strip():
                            list_data = dataline.strip().split(b'\t')
                        else:
                            continue
                        for data in list_data:
                            list_recdata = data.strip().decode('gbk').split(':')
                            if len(list_recdata) != 2:
                                list_recdata = data.strip().decode('gbk').split('：')
                            dict_rec_capital[list_recdata[0].strip()] = \
                                (lambda x: x if x.strip() in ['人民币'] else list_recdata[1].strip())(list_recdata[1])

            elif data_source_type in ['huat_matic_tsi']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_today)
                with open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if str(dict_capital['fund_account']) == acctidbybroker:
                                dict_rec_capital.update(dict_capital)

            elif data_source_type in ['gtja_fy'] and accttype in ['c', 'm']:
                wb = open_workbook(fpath, encoding_override='gbk')
                ws = wb.sheet_by_index(0)
                list_keys = ws.row_values(5)
                list_values = ws.row_values(6)
                dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['hait_ehtc'] and accttype == 'c':
                df_read = pd.read_excel(fpath, skiprows=1, nrows=1)
                dict_rec_capital = df_read.to_dict('records')[0]

            elif data_source_type in ['hait_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_capital = df_read.to_dict('records')[0]

            elif data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
                # todo 存在五粮液错误
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    dataline = list_datalines[0][8:]
                    list_recdata = dataline.strip().decode('gbk').split()
                    for recdata in list_recdata:
                        list_recdata = recdata.split(':')
                        dict_rec_capital.update({list_recdata[0]: list_recdata[1]})

            elif data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx'] and accttype in ['c', 'm']:
                # 已改为xls版本，避免'五粮液错误'
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().decode('gbk').replace('=', '').replace('"', '').split('\t')
                    list_values = list_datalines[1].strip().decode('gbk').replace('=', '').replace('"', '').split('\t')
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['zxjt_alphabee', 'swhy_alphabee'] and accttype in ['c', 'm']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_today)
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split()
                    list_values = list_datalines[1].decode('gbk').split()
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['swhy_alphabee_dbf2csv', 'ax_custom']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split(',')
                    list_values = list_datalines[1].decode('gbk').split(',')
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['patch']:
                pass

            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital_wealthcats = dict(zip(list_keys, list_values))
                            if dict_capital_wealthcats['账户'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital_wealthcats)

            elif data_source_type in ['gtja_pluto']:
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if dict_capital['单元序号'] == dlddata_filter:
                                dict_rec_capital.update(dict_capital)

            elif data_source_type in ['ax_jzpb']:
                # todo 账户编号不稳定，求源
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital_wealthcats = dict(zip(list_keys, list_values))
                            if dict_capital_wealthcats['账户编号'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital_wealthcats)

            elif data_source_type in ['zhaos_xtpb', 'db_xtpb_net', 'db_xtpb_smjxj', 'zxjt_xtpb', 'hf_xtpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if dict_capital['资金账号'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital)

            elif data_source_type in ['gs_htpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if dict_capital['资金账户'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital)

            else:
                raise ValueError('Field data_source_type not exist in basic info!')
            list_ret.append(dict_rec_capital)

        elif str_c_h_secliability_mark == 'holding':
            if data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
                # todo 存在五粮液错误
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    start_index_holding = None
                    for index, dataline in enumerate(list_datalines):
                        if '证券代码' in dataline.decode('gbk'):
                            start_index_holding = index
                    list_keys = [x.decode('gbk') for x in list_datalines[start_index_holding].strip().split()]
                    list_keys_2b_dropped = ['折算汇率', '备注', '历史成交', '资讯']
                    for key_2b_dropped in list_keys_2b_dropped:
                        if key_2b_dropped in list_keys:
                            list_keys.remove(key_2b_dropped)
                    i_list_keys_length = len(list_keys)

                    for dataline in list_datalines[start_index_holding + 1:]:
                        list_data = dataline.strip().split()
                        if len(list_data) == i_list_keys_length:
                            list_values = [x.decode('gbk') for x in list_data]
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx'] and accttype in ['c', 'm']:
                # 避免五粮液错误
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_list_data = [
                        dataline.decode('gbk').replace('=', '').replace('"', '').split('\t')
                        for dataline in list_datalines
                    ]
                    start_index_holding = None
                    for index, list_data in enumerate(list_list_data):
                        if '证券代码' in list_data:
                            start_index_holding = index
                    list_keys = list_list_data[start_index_holding]
                    i_list_keys_length = len(list_keys)
                    for list_values in list_list_data[start_index_holding + 1:]:
                        if '没有' in list_values[0]:
                            print(f'{acctidbybroker}: {list_values[0]}')
                        else:
                            if len(list_values) == i_list_keys_length:
                                dict_rec_holding = dict(zip(list_keys, list_values))
                                list_ret.append(dict_rec_holding)
                            else:
                                print(f'{acctidbybroker}_{data_source_type}_{list_values} not added into database')

            elif data_source_type in ['huat_hx', 'yh_hx', 'wk_hx', 'hait_hx',
                                      'zhes_hx', 'db_hx', 'tf_hx'] and accttype in ['c', 'm']:
                # 注： 证券名称中 有的有空格, 核新派以制表符分隔
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    start_index_holding = None
                    for index, dataline in enumerate(list_datalines):
                        if '证券代码' in dataline.decode('gbk'):
                            start_index_holding = index
                    list_keys = [x.decode('gbk') for x in list_datalines[start_index_holding].strip().split()]
                    list_keys_2b_dropped = ['折算汇率', '备注']
                    for key_2b_dropped in list_keys_2b_dropped:
                        if key_2b_dropped in list_keys:
                            list_keys.remove(key_2b_dropped)
                    i_list_keys_length = len(list_keys)

                    for dataline in list_datalines[start_index_holding + 1:]:
                        list_data = dataline.strip().split(b'\t')
                        if len(list_data) == i_list_keys_length:
                            list_values = [x.decode('gbk') for x in list_data]
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['hait_datagrp', 'yh_datagrp']:
                df_read = pd.read_excel(
                    fpath,
                    skiprows=3,
                    dtype={'股东代码': str},
                    converters={'代码': lambda x: str(x).zfill(6), '证券代码': lambda x: str(x).zfill(6)}
                )
                list_dicts_rec_holding = df_read.to_dict('records')
                list_ret = list_dicts_rec_holding

            elif data_source_type in ['gtja_fy'] and accttype in ['c', 'm']:
                wb = open_workbook(fpath, encoding_override='gbk')
                ws = wb.sheet_by_index(0)
                list_keys = ws.row_values(8)
                for i in range(9, ws.nrows):
                    list_values = ws.row_values(i)
                    if '' in list_values:
                        continue
                    str_values = ','.join(list_values)
                    if '合计' in str_values:
                        continue
                    dict_rec_holding = dict(zip(list_keys, list_values))
                    if accttype == 'm':
                        if '证券代码' in dict_rec_holding:
                            secid = dict_rec_holding['证券代码']
                            if secid[0] in ['0', '1', '3']:
                                dict_rec_holding['交易市场'] = '深A'
                            else:
                                dict_rec_holding['交易市场'] = '沪A'
                    list_ret.append(dict_rec_holding)

            elif data_source_type in ['hait_ehtc'] and accttype == 'c':
                wb_ehtc = load_workbook(fpath)
                ws = wb_ehtc.active
                i_target_row = 10
                for row in ws.rows:
                    for cell in row:
                        if cell.value == '持仓':
                            i_target_row = cell.row
                df_holding = pd.read_excel(fpath, skiprows=i_target_row)
                list_dicts_rec_holding = df_holding.to_dict('records')
                list_ret = list_dicts_rec_holding

            elif data_source_type in ['zxjt_alphabee', 'swhy_alphabee'] and accttype in ['c', 'm']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_today)
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split()
                    for dataline in list_datalines[1:]:
                        list_values = dataline.decode('gbk').split()
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['swhy_alphabee_dbf2csv', 'ax_custom'] and accttype in ['c', 'm']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[3].decode('gbk').split(',')
                    for dataline in list_datalines[4:]:
                        list_values = dataline.decode('gbk').split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['SymbolFull'].split('.')[1] == 'SZ':
                                dict_rec_holding['交易市场'] = '深A'
                            elif dict_rec_holding['SymbolFull'].split('.')[1] == 'SH':
                                dict_rec_holding['交易市场'] = '沪A'
                            else:
                                raise ValueError('Unknown exchange mark.')
                            if dict_rec_holding['账户'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['huat_matic_tsi']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_today)
                with open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if str(dict_rec_holding['fund_account']) == acctidbybroker:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['ax_jzpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['账户编号'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['gtja_pluto']:
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['单元序号'] == dlddata_filter:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['zhaos_xtpb', 'db_xtpb_net', 'db_xtpb_smjxj', 'zxjt_xtpb', 'hf_xtpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账号'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['gs_htpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

        elif str_c_h_secliability_mark == 'secliability':
            if data_source_type in ['zhaos_xtpb'] and accttype in ['m']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    start_index_secliability = None
                    for index, dataline in enumerate(list_datalines):
                        str_dataline = dataline.decode('gbk')
                        if '证券代码' in str_dataline:
                            start_index_secliability = index
                    list_keys = [x.decode('gbk') for x in list_datalines[start_index_secliability].strip().split()]
                    i_list_keys_length = len(list_keys)
                    for dataline in list_datalines[start_index_secliability + 1:]:
                        list_data = dataline.strip().split()
                        if len(list_data) == i_list_keys_length:
                            list_values = [x.decode('gbk') for x in list_data]
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            # todo 自定义： 根据证券代码推测交易市场
                            secid = dict_rec_holding['证券代码']
                            if secid[0] in ['0', '1', '3']:
                                dict_rec_holding['交易市场'] = '深A'
                            else:
                                dict_rec_holding['交易市场'] = '沪A'
                            list_ret.append(dict_rec_holding)
        else:
            raise ValueError('Wrong str_c_h_secliability_mark input!')
        return list_ret

    def update_rawdata(self):
        """
        1. 出于数据处理留痕及增强robust考虑，将原始数据按照原格式上传到mongoDB中备份
        2. 定义DataFilePath = ['fpath_capital_data'(source), 'fpath_holding_data'(source), 'fpath_trdrec_data(source)',]
        3. acctinfo数据库中DataFilePath存在文件路径即触发文件数据的上传。
        4. 添加：融券未平仓合约数据的上传
        """
        col_manually_rawdata_capital = self.db_trddata['manually_rawdata_capital']
        col_manually_rawdata_holding = self.db_trddata['manually_rawdata_holding']
        col_manually_rawdata_secliability = self.db_trddata['manually_rawdata_secliability']
        for _ in self.col_acctinfo.find({'DataDate': self.str_today, 'RptMark': 1}, {'_id': 0}):
            datafilepath = _['DataFilePath']
            if datafilepath:
                list_fpath_data = _['DataFilePath'][1:-1].split(',')
                acctidbymxz = _['AcctIDByMXZ']
                acctidbybroker = _['AcctIDByBroker']
                data_source_type = _['DataSourceType']
                accttype = _['AcctType']
                dlddata_filter = _['DownloadDataFilter']

                for ch in ['capital', 'holding', 'secliability']:
                    if ch == 'capital':
                        fpath_relative = list_fpath_data[0]
                        col_manually_rawdata = col_manually_rawdata_capital
                    elif ch == 'holding':
                        fpath_relative = list_fpath_data[1]
                        col_manually_rawdata = col_manually_rawdata_holding
                    elif ch == 'secliability':
                        if len(list_fpath_data) > 2:
                            fpath_relative = list_fpath_data[2]
                            if fpath_relative:
                                col_manually_rawdata = col_manually_rawdata_secliability
                            else:
                                continue
                        else:
                            continue
                    else:
                        raise ValueError('Value input not exist in capital and holding.')

                    col_manually_rawdata.delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz})
                    fpath_absolute = os.path.join(self.dirpath_data_from_trdclient, fpath_relative)
                    list_dicts_rec = self.read_rawdata_from_trdclient(
                        fpath_absolute, ch, data_source_type, accttype, acctidbybroker, dlddata_filter
                    )
                    for _ in list_dicts_rec:
                        _['DataDate'] = self.str_today
                        _['AcctIDByMXZ'] = acctidbymxz
                        _['AcctType'] = accttype
                    if list_dicts_rec:
                        col_manually_rawdata.insert_many(list_dicts_rec)
        print('Update raw data finished.')

    def update_trddata_f(self):
        cursor_find = list(self.col_acctinfo.find({'DataDate': self.str_today, 'AcctType': 'f', 'DataDownloadMark': 1}))
        for _ in cursor_find:
            list_future_data_capital = []
            list_future_data_holding = []
            list_future_data_trdrec = []
            prdcode = _['PrdCode']
            acctidbymxz = _['AcctIDByMXZ']
            acctidbyowj = _['AcctIDByOuWangJiang4FTrd']
            trader = Trader(acctidbyowj)
            dict_res_capital = trader.query_capital()
            if dict_res_capital:
                dict_capital_to_be_update = dict_res_capital
                dict_capital_to_be_update['DataDate'] = self.str_today
                dict_capital_to_be_update['AcctIDByMXZ'] = acctidbymxz
                dict_capital_to_be_update['AcctIDByOWJ'] = acctidbyowj
                dict_capital_to_be_update['PrdCode'] = prdcode
                list_future_data_capital.append(dict_capital_to_be_update)
                self.db_trddata['future_api_capital'].delete_many(
                    {'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz}
                )
                if list_future_data_capital:
                    self.db_trddata['future_api_capital'].insert_many(list_future_data_capital)

            list_list_res_holding = trader.query_holding()
            list_keys_holding = [
                'exchange', 'instrument_id', 'direction', 'hedge', 'position', 'position_td', 'open_volume',
                'close_volume', 'unknown1', 'unknown2', 'unknown3'
            ]
            if len(list_list_res_holding):
                list_dicts_holding_to_be_update = list_list_res_holding
                for list_holding_to_be_update in list_dicts_holding_to_be_update:
                    dict_holding_to_be_update = dict(zip(list_keys_holding, list_holding_to_be_update))
                    dict_holding_to_be_update['DataDate'] = self.str_today
                    dict_holding_to_be_update['AcctIDByMXZ'] = acctidbymxz
                    dict_holding_to_be_update['AcctIDByOWJ'] = acctidbyowj
                    dict_holding_to_be_update['PrdCode'] = prdcode
                    list_future_data_holding.append(dict_holding_to_be_update)

                self.db_trddata['future_api_holding'].delete_many({'DataDate': self.str_today,
                                                                   'AcctIDByMXZ': acctidbymxz})
                if list_future_data_holding:
                    self.db_trddata['future_api_holding'].insert_many(list_future_data_holding)

            list_list_res_trdrecs = trader.query_trdrecs()
            if len(list_list_res_trdrecs):
                list_keys_trdrecs = ['instrument_id', 'direction', 'offset', 'volume', 'price', 'time', 'trader']
                for list_res_trdrecs in list_list_res_trdrecs:
                    dict_trdrec = dict(zip(list_keys_trdrecs, list_res_trdrecs))
                    dict_trdrec['DataDate'] = self.str_today
                    dict_trdrec['AcctIDByMXZ'] = acctidbymxz
                    dict_trdrec['AcctIDByOWJ'] = acctidbyowj
                    dict_trdrec['PrdCode'] = prdcode
                    list_future_data_trdrec.append(dict_trdrec)
                self.db_trddata['future_api_trdrec'].delete_many(
                    {'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz}
                )
                if list_future_data_trdrec:
                    self.db_trddata['future_api_trdrec'].insert_many(list_future_data_trdrec)
            print(f'{acctidbymxz} update finished!')

    def update_manually_patchdata(self):
        """
        从data_patch中读取当日数据。
        注意，data_patch中只记录最新的当日数据
        注意，此步骤处理了原数据，将加工后的数据上传至数据库
        证券市值净值：SecurityMarketValue_vec: 是矢量，有正负
        """
        # 上传holding data
        dict_windcode2mktdata_from_wind = self.df_mktdata_from_wind.set_index('WindCode').to_dict()
        df_datapatch_holding = pd.read_excel(self.fpath_datapatch_relative, sheet_name='holding',
                                             dtype={'AcctIDByMXZ': str, 'SecurityID': str, 'Symbol': str,
                                                    'LongQty': float, 'ShortQty': float, 'CashFromShortSelling': float,
                                                    'LiabilityType': str,
                                                    'LiabilityQty': float, 'LiabilityAmt': float, 'InterestRate': float,
                                                    'DatedDate': str, 'UnderlyingSecurityID': str,
                                                    'UnderlyingSecurityIDSource': str, 'UnderlyingSymbol': str,
                                                    'UnderlyingQty': float, 'UnderlyingStartValue_vec': float,
                                                    'Note': str},
                                             converters={'SecurityIDSource': str.upper})
        df_datapatch_holding = df_datapatch_holding.where(df_datapatch_holding.notnull(), None)
        list_dicts_datapatch_holding = df_datapatch_holding.to_dict('records')
        dict_exchange_wcode = {'SSE': '.SH', 'SZSE': '.SZ', 'CFFEX': '.CFE'}
        list_dicts_datapatch_holding_to_be_inserted = []
        for dict_datapatch_holding in list_dicts_datapatch_holding:
            acctidbymxz = dict_datapatch_holding['AcctIDByMXZ']
            secid = dict_datapatch_holding['SecurityID']
            secidsrc = dict_datapatch_holding['SecurityIDSource']
            symbol = dict_datapatch_holding['Symbol']
            secid_secidsrc = secid + '.' + secidsrc
            if secidsrc == 'ITN':
                secid_windcode = None
            else:
                secid_windcode = secid + dict_exchange_wcode[secidsrc]
            longqty = dict_datapatch_holding['LongQty']
            shortqty = dict_datapatch_holding['ShortQty']
            sectype = self.get_mingshi_sectype_from_code(secid_secidsrc)
            cash_from_ss = dict_datapatch_holding['CashFromShortSelling']
            liability_sec_qty = dict_datapatch_holding['LiabilityQty']
            liability_margin_amt = dict_datapatch_holding['LiabilityAmt']
            liability_type = dict_datapatch_holding['LiabilityType']
            interest_rate = dict_datapatch_holding['InterestRate']
            dateddate = dict_datapatch_holding['DatedDate']
            liability = 0
            note = dict_datapatch_holding['Note']

            underlying_secid = None
            underlying_secidsrc = None
            underlying_symbol = None
            underlying_sectype = None
            underlying_qty = None
            underlying_close = None
            underlying_amt = None
            underlying_start_value_vec = None
            otc_contract_unit_mv = None

            if sectype in ['SWAP']:
                underlying_secid = dict_datapatch_holding['UnderlyingSecurityID']
                underlying_secidsrc = dict_datapatch_holding['UnderlyingSecurityIDSource']
                underlying_sec_windcode = underlying_secid + dict_exchange_wcode[underlying_secidsrc]
                underlying_symbol = dict_windcode2mktdata_from_wind['Symbol'][underlying_sec_windcode]
                underlying_sectype = self.get_mingshi_sectype_from_code(underlying_sec_windcode)
                underlying_close = dict_windcode2mktdata_from_wind['Close'][underlying_sec_windcode]
                underlying_qty = dict_datapatch_holding['UnderlyingQty']
                underlying_start_value_vec = dict_datapatch_holding['UnderlyingStartValue_vec']

                if underlying_sectype in ['Index Future']:
                    dict_index_future2multiplier = {'IC': 200, 'IF': 300, 'IH': 300}
                    underlying_amt = (underlying_close
                                      * underlying_qty
                                      * dict_index_future2multiplier[underlying_secid[:2]])
                else:
                    underlying_amt = underlying_close * underlying_qty

                otc_contract_unit_mv = underlying_amt - underlying_start_value_vec
                close = otc_contract_unit_mv

            elif sectype in ['CE', 'CS', 'ETF']:
                close = dict_windcode2mktdata_from_wind['Close'][secid_windcode]
                if sectype in ['CS', 'ETF']:
                    int_accumulated_interested_days = w.tdayscount(dateddate, self.str_today, "Days=Alldays").Data[0][0]
                    if liability_type == 'capital':
                        interest_1day = liability_margin_amt * interest_rate / 365
                        liability_accrual_interest = int_accumulated_interested_days * interest_1day
                        liability = liability_margin_amt + liability_accrual_interest
                    elif liability_type == 'security':  # todo 不标准的表述, 需改进; 出于开发成本考虑，此处简化融券应付利息算法
                        liability_sec_amt = liability_sec_qty * close
                        interest_1day = liability_sec_amt * interest_rate / 365
                        liability = liability_sec_amt + int_accumulated_interested_days * interest_1day
                    else:
                        raise ValueError('Unknown liability type when compute liability')
            else:
                raise ValueError('Unknown sectype when update manually data.')

            longamt = close * longqty
            shortamt = close * shortqty
            netamt = longamt - shortamt

            dict_datapatch_holding_2b_inserted = {
                'DataDate': self.str_today,
                'AcctIDByMXZ': acctidbymxz,
                'SecurityID': secid,
                'SecurityIDSource': secidsrc,
                'Symbol': symbol,
                'SecurityType': sectype,
                'LongQty': longqty,
                'ShortQty': shortqty,
                'LongAmt': longamt,
                'ShortAmt': shortamt,
                'NetAmt': netamt,
                'CashFromShortSelling': cash_from_ss,
                'LiabilityType': liability_type,
                'LiabilityQty': liability_sec_qty,
                'LiabilityAmt': liability_margin_amt,
                'InterestRate': interest_rate,
                'DatedDate': dateddate,
                'Liability': liability,
                'UnderlyingSecurityID': underlying_secid,
                'UnderlyingSecurityIDSource': underlying_secidsrc,
                'UnderlyingSymbol': underlying_symbol,
                'UnderlyingSecurityType': underlying_sectype,
                'UnderlyingQty': underlying_qty,
                'UnderlyingClose': underlying_close,
                'UnderlyingAmt': underlying_amt,
                'UnderlyingStartValue_vec': underlying_start_value_vec,
                'OTCContractUnitMarketValue': otc_contract_unit_mv,
                'Note': note
            }
            list_dicts_datapatch_holding_to_be_inserted.append(dict_datapatch_holding_2b_inserted)
        self.db_trddata['manually_patchdata_holding'].delete_many({'DataDate': self.str_today})
        self.db_trddata['manually_patchdata_holding'].insert_many(list_dicts_datapatch_holding_to_be_inserted)

        # 上传capital data
        df_datapatch_capital = pd.read_excel(self.fpath_datapatch_relative, sheet_name='capital',
                                             dtype={'AcctIDByMXZ': str, 'Cash': float, 'CashEquivalent': float,
                                                    'ETFLongAmt': float, 'CompositeLongAmt': float, 'OTCAmt': float,
                                                    'TotalAsset': float, 'CapitalDebt': float, 'ETFShortAmt': float,
                                                    'CompositeSHortAmt': float, 'Liability': float,
                                                    'ApproximateNetAsset': float})
        df_datapatch_capital['DataDate'] = self.str_today
        df_datapatch_capital = df_datapatch_capital.where(df_datapatch_capital.notnull(), None)
        list_dicts_patch_capital = df_datapatch_capital.to_dict('record')
        self.db_trddata['manually_patchdata_capital'].delete_many({'DataDate': self.str_today})
        if list_dicts_patch_capital:
            self.db_trddata['manually_patchdata_capital'].insert_many(list_dicts_patch_capital)

        # 上传划款数据
        """
        Abbr.:
            1. DW: Deposit and Withdraw, 产品出入金的事项。
            2. 事项包括：
                1. S: Subscribe 认购
                2. P: Purchase 申购
                2. R: Redemption 赎回
                3. DD: Dividends Distribution 分红
        思路：
            1.更新SPR事项的原始数据表，按照表中的当日数据更新。
            2.可以对遗漏事项进行补充，DataDate为当日，NoticeDateTime为往日。
            3.本函数直接根据原始数据计算出可用数据。
            4.划款事项数据的主键为： NoticeDateTime, PrdCode, DWItem, CapitalSource, UNAVDate
        假设：
            1. 真正对交易计划产生影响的是申赎款事项的状态。
            2. 部分划款-已划款状况的处理。
        字段说明：
            0. DWItemsID: 由日期和4位代码升序代码组成，eg: 202007240001
            1. Amt, Shares, DWedAmt为矢量, 是原始表中的数据
            2. UNAVConfirmationDate: 申赎依据该净值确认日期确认。 
            3. 计算出来的字段有： 
                1. LatestUNAVRptDate, 最新报告净值期日
                2. UNAVOnConfirmationDate, 净值确认日的净值
                3. DWBgtUNAV: 划款事项预算净值
                   读取状态码，当状态码为2/3时，
                   如有申赎确认日净值，则使用申赎确认日净值，
                   否则使用分红单位净值。
                2. DWBGTNetAMT, 为该事项的应划款净额(事项的历史合计数)。
                   估算值（谨慎估算），矢量值，为定值，不受部分已划款金额的影响。
                3. DWBGTNetAMT2DW, 为该事项当前的待划款金额的影响
                    重要，该值对预算净值产生直接影响。
                    Amt2DW = DWBGTNetAMT - AmtDWed
                 
            4. Status:
                描述DW事项的状态
                DW状态为状态流上的节点，依次为：
                已通知/待生效 → 
                int: {
                    0: 结束,
                    1: 已通知，待生效,
                    2: 已生效，待划款,
                    3: 部分划款,
                    4: 全部划款,
                }
            
        5. 在写入数据库时，读取excel中的所有数据，将状态码大于0的事项写入。即在excel中，可删除status为0的条目。
            本质上为全量写入，但是由于从企业微信抓取的数据可能为重复事项，出于谨慎性考虑，故保留此步
        6. DW事项可在任意节点强制取消，故由人控制状态码，此步不可少
            
        """
        # 读取清算净值数据：
        # \\192.168.4.121\data\Final_new\20200724
        fpath_df_unav_from_4121_final_new = (f'//192.168.4.121/data/Final_new/{self.str_today}/'
                                             f'######产品净值相关信息######.xlsx')

        # \\192.168.4.121\data\Final_new\20200724
        # fpath_df_unav_from_4121_final_new = ('//192.168.4.121/data/Final_new/20201014/######产品净值相关信息######.xlsx')

        if not os.path.exists(fpath_df_unav_from_4121_final_new):
            fpath_df_unav_from_4121_final_new = (f'//192.168.4.121/data/Final_new/{self.str_last_trddate}/'
                                                 f'######产品净值相关信息######.xlsx')

        # 注意读取的最新日期有可能会改变格式，目前为-
        df_unav_from_4121_final_new = pd.read_excel(
            fpath_df_unav_from_4121_final_new,
            dtype={
                '产品编号': str,
                '产品名称': str,
                '最新更新日期': str,
                '分红单位净值': float,
            }
        )
        list_dicts_unavs_from_4121_final_new = df_unav_from_4121_final_new.to_dict('records')

        df_datapatch_dwitems_read_excel = pd.read_excel(
            self.fpath_datapatch_relative,
            sheet_name='dwitems',
            dtype={
                'DataDate': str,
                'DWItemsID': str,
                'NoticeDateTime': str,
                'PrdCode': str,
                'UNAVDate': str,
                'Amt': float,
                'Shares': float,
                'ExpectedDWDate': str,
                'Status': int,
                'DWedAmt': float,
                'EffectiveDate': str,
                'UNAVConfirmationDate': str,
            }
        )
        df_datapatch_dwitems_read_excel = (df_datapatch_dwitems_read_excel
                                           .where(df_datapatch_dwitems_read_excel.notnull(), None))
        list_dicts_dwitems_read_excel = df_datapatch_dwitems_read_excel.to_dict('records')
        list_dicts_dwitems_2b_inserted = []
        for dict_dwitems_read_excel in list_dicts_dwitems_read_excel:
            unav_confirmation_date = dict_dwitems_read_excel['UNAVConfirmationDate']
            unav_on_confirmation_date = None
            latest_unav_in_final_new = None
            prdcode = dict_dwitems_read_excel['PrdCode']
            dict_prdinfo = self.col_prdinfo.find_one({'DataDate': self.str_today, 'PrdCode': prdcode})
            prdcode_in_4121_final_new = dict_prdinfo['PrdCodeIn4121FinalNew']
            dwamt = dict_dwitems_read_excel['Amt']
            if dwamt is None:
                dwamt = 0
            dwshares = dict_dwitems_read_excel['Shares']
            dwed_amt = dict_dwitems_read_excel['DWedAmt']
            if not dwed_amt:
                dwed_amt = 0
            if dwshares is None:
                dwshares = 0
            for dict_unav_from_4121_final_new in list_dicts_unavs_from_4121_final_new:
                prdcode_in_final_new = dict_unav_from_4121_final_new['产品编号']
                if prdcode_in_final_new == prdcode_in_4121_final_new:
                    latest_unav_date_in_final_new = dict_unav_from_4121_final_new['最新更新日期'].replace('-', '')
                    latest_unav_in_final_new = dict_unav_from_4121_final_new['分红单位净值']
                    dict_dwitems_read_excel['LatestUNAVRptDate'] = latest_unav_date_in_final_new
                    dict_dwitems_read_excel['LatestRptUNAV'] = latest_unav_in_final_new
                    if unav_confirmation_date <= latest_unav_date_in_final_new:
                        dict_prdinfo = self.col_prdinfo.find_one(
                            {'DataDate': unav_confirmation_date, 'PrdCodeIn4121FinalNew': prdcode_in_final_new}
                        )
                        unav_on_confirmation_date = dict_prdinfo['UNAVFromLiquidationRpt']
            if unav_on_confirmation_date:
                dwbgt_unav = unav_on_confirmation_date
            else:
                dwbgt_unav = latest_unav_in_final_new
            dwbgt_netamt = dwamt + dwshares * dwbgt_unav

            if dwshares < 0:
                dwbgt_netamt_estimated = dwamt + dwshares * (dwbgt_unav + 0.01)
            else:
                dwbgt_netamt_estimated = dwamt + dwshares * dwbgt_unav

            # todo 时间设计是否有用？
            # str_notice_dt = dict_dwitems_read_excel['NoticeDateTime']
            # str_notice_date = str_notice_dt.split('T')[0]
            # str_notice_time = str_notice_dt.split('T')[1]
            #
            # dict_dwitems_read_excel['NoticeDate'] = str_notice_date
            # dict_dwitems_read_excel['NoticeTime'] = str_notice_time
            dict_dwitems_read_excel['DataDate'] = self.str_today
            dict_dwitems_read_excel['Shares'] = dwshares
            dict_dwitems_read_excel['UNAVOnConfirmationDate'] = unav_on_confirmation_date
            dict_dwitems_read_excel['UNAVOnLatestRptDate'] = latest_unav_in_final_new
            dict_dwitems_read_excel['DWBGTNetAmt'] = dwbgt_netamt
            dict_dwitems_read_excel['DWBGTNetAmtEstimated'] = dwbgt_netamt_estimated
            dict_dwitems_read_excel['DWBGTNetAMTEstimated2DW'] = dwbgt_netamt_estimated - dwed_amt
            status = dict_dwitems_read_excel['Status']
            if status > 0:
                list_dicts_dwitems_2b_inserted.append(dict_dwitems_read_excel)

        self.db_trddata['manually_patchdata_dwitems'].delete_many({'DataDate': self.str_today})
        if list_dicts_dwitems_2b_inserted:
            self.db_trddata['manually_patchdata_dwitems'].insert_many(list_dicts_dwitems_2b_inserted)

        print('Collection manually_patchdata_capital and collection manually_patchdata_holding updated.')

    def update_na_allocation(self):
        """
        计算产品渠道比例
        可以在datapatch中重新指定比例
        todo 比例的自动计算：
        1. 读前一天的比例
        2. 筛选出effective date为今天的划款并汇总，
        3. 读取当前净资产值
        4. 计算比例，写入数据库
        5. 将指定比例写入数据库
        """

        list_dicts_prdinfo = list(self.col_prdinfo.find({'DataDate': self.str_today}))
        list_dicts_na_allocation = []
        for dict_prdinfo in list_dicts_prdinfo:
            prdcode = dict_prdinfo['PrdCode']
            dict_na_allocation = dict_prdinfo['NetAssetAllocation']
            if dict_na_allocation:
                if 'security' in dict_na_allocation:
                    dict_na_allocation_saccts = dict_na_allocation['security']
                else:
                    dict_na_allocation_saccts = None
                if 'future' in dict_na_allocation:
                    dict_na_allocation_faccts = dict_na_allocation['future']
                else:
                    dict_na_allocation_faccts = None
                dict_na_allocation = {
                    'DataDate': self.str_today,
                    'PrdCode': prdcode,
                    'SecurityAccountsNetAssetAllocation': dict_na_allocation_saccts,
                    'FutureAccountsNetAssetAllocation': dict_na_allocation_faccts,
                }
                list_dicts_na_allocation.append(dict_na_allocation)

        self.db_trddata['na_allocation'].delete_many({'DataDate': self.str_today})
        if list_dicts_na_allocation:
            self.db_trddata['na_allocation'].insert_many(list_dicts_na_allocation)
        print('Update na allocation finished')

    def get_close_from_wind(self):
        """
        停牌股票为前收价
        """
        wset_astock = w.wset("sectorconstituent", f"date={self.str_today};sectorid=a001010100000000")
        list_str_wcodes_astock = wset_astock.Data[1]
        # todo 可改进
        list_wcodes_query_patch = ['510500.SH', '511660.SH', '512500.SH', '000905.SH', '000300.SH', '000016.SH',
                                   'IC2009.CFE', 'IC2012.CFE']
        list_str_wcodes_astock += list_wcodes_query_patch
        str_wcodes_astock = ','.join(list_str_wcodes_astock)
        wss_astock = w.wss(str_wcodes_astock, 'sec_name,close', f'tradeDate={self.str_today};priceAdj=U;cycle=D')
        df_mktdata_from_wind = pd.DataFrame(
            wss_astock.Data, index=wss_astock.Fields, columns=wss_astock.Codes
        ).T.reset_index().rename(columns={'index': 'WindCode', 'SEC_NAME': 'Symbol', 'CLOSE': 'Close'})
        list_dicts_mktpatch = [
            {'WindCode': '204001.SH', 'Symbol': 'GC001', 'Close': 100},  # 迅投PB 单价100，华泰核心，单价1000
            {'WindCode': '511990.SH', 'Symbol': '华宝添益', 'Close': 100},
            {'WindCode': '511880.SH', 'Symbol': '银河日利', 'Close': 100},
            {'WindCode': '159001.SZ', 'Symbol': '保证金', 'Close': 100}
        ]
        df_mktdata_from_wind = df_mktdata_from_wind.append(list_dicts_mktpatch)
        return df_mktdata_from_wind

    @staticmethod
    def get_official_sectype_from_code(str_code):
        """
        未使用的函数，当作留底参考
        输入windcode， 返回该标的类型
        参考：
            1.上海证券交易所证券代码分配指南
            2.深圳证券交易所数据接口规范ver 4.72_201505
        """
        list_split_wcode = str_code.split('.')
        secid = list_split_wcode[0]
        exchange = list_split_wcode[1]
        if exchange in ['SH', 'SSE'] and len(secid) == 6:
            if secid[0] == '0':
                return '指数、国债'
            elif secid[0] == '1':
                return '债券'
            elif secid[0] == '2':
                return '债券回购'
            elif secid[0] == '3':
                return '优先股'
            elif secid[0] == '5':
                dict_map = {'500': '契约型封闭式基金', '501': '上市开放式基金', '502': '上市开放式基金',
                            '505': '创新型封闭式证券投资基金', '510': '交易型开放式指数证券投资基金',
                            '511': '债券交易型开放式指数基金、交易型货币市场基金', '512': '交易型开放式指数证券投资基金',
                            '513': '交易型开放式指数证券投资基金', '515': '交易型开放式指数证券投资基金',
                            '518': '商品交易型开放式证券投资基金', '519': '开放式基金申赎', '521': '开放式基金跨市场转托管',
                            '522': '开放式基金跨市场转托管', '523': '开放式基金分红', '524': '开放式基金基金转换',
                            '550': '基金', '580': '权证', '582': '权证行权'}
                return dict_map[secid[:3]]
            elif secid[:3] in ['600', '601', '603', '688']:
                return 'A股'
            elif secid[:3] == '689':
                return '存托凭证'
            elif secid[0] == '7':
                return '非交易业务'
            elif secid[0] == '8':
                return '标准券、备用'
            elif secid[0] == '9':
                return 'B股'
        elif exchange in ['SZ', 'SZSE'] and len(secid) == 6:
            dict_map = {
                '000': '主板A股',
                '001': '主板A股',
                '002': '中小企业板股票',
                '003': '中小企业板股票',
                '004': '中小企业板股票',
                '030': '主板A股及中小企业板股票认购权证',
                '031': '主板A股及中小企业板股票认购权证',
                '032': '主板A股及中小企业板股票认购权证',
                '036': '创业板股权激励计划涉及的员工认股权',
                '037': '员工认股权',
                '038': '员工认股权',
                '039': '员工认股权',
                '070': '主板A股增发,主板可转换公司债券申购',
                '071': '主板A股增发,主板可转换公司债券申购',
                '072': '中小企业板股票增发,中小企业板可转换公司债券申购',
                '073': '中小企业板股票增发,中小企业板可转换公司债券申购',
                '074': '中小企业板股票增发,中小企业板可转换公司债券申购',
                '080': '主板A股配股优先权,主板可转换公司债券的优先权认购',
                '081': '主板A股配股优先权,主板可转换公司债券的优先权认购',
                '082': '中小企业板配股优先权,中小企业板可转换公司债券的优先权认购',
                '083': '中小企业板配股优先权,中小企业板可转换公司债券的优先权认购',
                '084': '中小企业板配股优先权,中小企业板可转换公司债券的优先权认购',
                '115': '可分离交易的可转换公司债',
                '120': '可转换公司债券',
                '121': '可转换公司债券',
                '122': '可转换公司债券',
                '123': '可转换公司债券',
                '124': '可转换公司债券',
                '125': '可转换公司债券',
                '126': '可转换公司债券',
                '127': '可转换公司债券',
                '128': '可转换公司债券',
                '129': '可转换公司债券',
                '131': '债券回购',
                '140': '优先股',
                '150': '分级基金子基金',
                '151': '分级基金子基金',
                '159': 'ETF',
                '160': '开放式基金',
                '161': '开放式基金',
                '162': '开放式基金',
                '163': '开放式基金',
                '164': '开放式基金',
                '165': '开放式基金',
                '166': '开放式基金',
                '167': '开放式基金',
                '168': '开放式基金',
                '169': '开放式基金',
                '184': '封闭式证券投资基金',
                '300': '创业板股票',
                '301': '创业板股票',
                '302': '创业板股票',
                '303': '创业板股票',
                '304': '创业板股票',
                '305': '创业板股票',
                '306': '创业板股票',
                '307': '创业板股票',
                '308': '创业板股票',
                '309': '创业板股票',
                '370': '创业板股票增发,创业板可转换公司债券申购',
                '371': '创业板股票增发,创业板可转换公司债券申购',
                '372': '创业板股票增发,创业板可转换公司债券申购',
                '373': '创业板股票增发,创业板可转换公司债券申购',
                '374': '创业板股票增发,创业板可转换公司债券申购',
                '375': '创业板股票增发,创业板可转换公司债券申购',
                '376': '创业板股票增发,创业板可转换公司债券申购',
                '377': '创业板股票增发,创业板可转换公司债券申购',
                '378': '创业板股票增发,创业板可转换公司债券申购',
                '379': '创业板股票增发,创业板可转换公司债券申购',
                '399': '指数'
            }
            return dict_map[secid[:3]]
        else:
            raise ValueError(f'Cannot get security type from code input: {str_code}')

    @staticmethod
    def get_mingshi_sectype_from_code(str_code):
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

    def update_formatted_holding_and_balance_sheet_and_exposure_analysis(self):
        """
        从原始数据表中读取数据，转换为内部格式并入库，以AcctIDByMXZ作为表名
        需要找到字段名称的映射
        操作层上，以实体账户为单位进行上传， 对于单个账户：
            1. 传入rawdata，并format
            2. 根据acctinfo中的patchmark判定是否需要patch
            3. 从数据库中读取patchdata
            4. 整理patchdata并对formatted data进行修正
            3. 整合并格式化

        内部格式数据库字段说明：
            1. col_capital:
                1. AvailableFund: 可用资金
                2. SecurityMarketValue: 证券市值
                3. TotalAsset: 总资产
                4. TotalLiability: 总负债
                5. NetAsset: 净资产  (计算得出)

            2. col_holding:
                0. DataDate
                1. AcctIDByMXZ
                1. SecurityID
                2. SecurityIDSource
                3. SecurityType
                4. Close
                5. LongQty
                6. ShortQty
                7. LongAmt
                8. ShortAmt
        循环嵌套：
            对每个账户遍历
                1. 求单账户风险暴露： [c,m,f,o]
                2. 求单账户持仓状况和资产负债表
                3. 求单账户现金管理
        Note：
            1. 出于业务化简考虑，cash from short selling 字段，在macct中，统一为0,
            2. 由于oacct融券业务规则未知，故oacct中cash from ss 为None
        todo：
            1. 新股的市值的处理（目前: 找出并删除），需要观察经纪商是如何处理的
            2. 本函数有命名混淆：数据表中的holding包含了空头持仓信息，而实际算法中，holding实际是多头持仓。
               FIX协议中，对此区分为两部分，holding 为多头，position为多头与空头的汇总。此处可改进
        """

        dict_wcode2close = self.df_mktdata_from_wind.set_index('WindCode').to_dict()['Close']

        list_dicts_acctinfo = list(self.col_acctinfo.find({'DataDate': self.str_today, 'RptMark': 1}, {'_id': 0}))
        for dict_acctinfo in list_dicts_acctinfo:
            prdcode = dict_acctinfo['PrdCode']
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            if accttype in ['c', 'm', 'o']:
                patchmark = dict_acctinfo['PatchMark']
                # 1.整理holding
                # 1.1 rawdata(无融券合约账户)
                list_dicts_holding = list(self.db_trddata['manually_rawdata_holding'].find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}, {'_id': 0}
                ))
                list_fields_secid = ['代码', '证券代码', 'stock_code']
                list_fields_symbol = ['证券名称', 'stock_name']
                list_fields_shareholder_acctid = ['股东帐户', '股东账号', '股东代码']
                list_fields_exchange = ['市场', '市场代码', '交易市场', '交易板块', '板块', '交易所', '交易所名称', '交易市场',
                                        'exchange_type']

                # 有优先级别的列表
                list_fields_longqty = [
                    '股票余额', '拥股数量', '证券余额', '证券数量', '库存数量', '持仓数量', '参考持股', '持股数量', '当前持仓',
                    '当前余额', '当前拥股', '实际数量', '实时余额', 'current_amount'
                ]
                list_dicts_holding_fmtted = []
                for dict_holding in list_dicts_holding:
                    secid = None
                    secidsrc = None
                    symbol = None
                    longqty = 0
                    shortqty = 0
                    for field_secid in list_fields_secid:
                        if field_secid in dict_holding:
                            secid = str(dict_holding[field_secid])

                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_holding:
                            shareholder_acctid = str(dict_holding[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_holding:
                            exchange = dict_holding[field_exchange]
                            dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE',
                                                      '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                                                      '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                                                      '00': 'SZSE', '10': 'SSE',
                                                      '0': 'SZSE', '1': 'SSE', '2': 'SZSE',
                                                      '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                                                      '上海A股': 'SSE', '深圳A股': 'SZSE',
                                                      'SH': 'SSE', 'SZ': 'SZSE',
                                                      '上交所A': 'SSE', '深交所A': 'SZSE',
                                                      }
                            secidsrc = dict_exchange2secidsrc[exchange]
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_holding:
                            symbol = str(dict_holding[field_symbol])

                    for field_longqty in list_fields_longqty:
                        if field_longqty in dict_holding:
                            longqty = float(dict_holding[field_longqty])

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = self.get_mingshi_sectype_from_code(windcode)
                    if sectype == 'IrrelevantItem':
                        continue
                    if windcode in dict_wcode2close:
                        close = dict_wcode2close[windcode]
                    else:
                        print(f'{windcode} not found in dict_wcode2close.')
                        close = 0
                    if close is None:
                        print(f'{windcode} not found in dict_wcode2close.')
                        close = 0
                    longamt = close * longqty
                    shortamt = 0
                    netamt = longamt - shortamt

                    if accttype in ['c', 'f', 'o']:
                        cash_from_ss_in_holding_fmtted = None
                    elif accttype in ['m']:
                        cash_from_ss_in_holding_fmtted = 0
                    else:
                        raise ValueError('Unknown accttype.')

                    dict_holding_fmtted = {
                        'DataDate': self.str_today,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'LongAmt': longamt,
                        'ShortAmt': shortamt,
                        'NetAmt': netamt,
                        'CashFromShortSelling': cash_from_ss_in_holding_fmtted,
                        'OTCContractUnitMarketValue': None,
                        'LiabilityType': None,
                        'Liability': 0,
                        'LiabilityQty': None,  # 融券数量
                        'LiabilityAmt': None,  # 融资数量
                        'InterestRate': None,
                        'DatedDate': None,
                        'UnderlyingSecurityID': None,
                        'UnderlyingSecurityIDSource': None,
                        'UnderlyingSecurityType': None,
                        'UnderlyingSymbol': None,
                        'UnderlyingQty': None,
                        'UnderlyingAmt': 0,
                        'UnderlyingClose': None,
                        'UnderlyingStartValue_vec': None,
                        'Note': None
                    }
                    list_dicts_holding_fmtted.append(dict_holding_fmtted)

                # 处理融券合约账户
                list_dicts_secliability = list(self.db_trddata['manually_rawdata_secliability'].find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}, {'_id': 0}
                ))
                shortqty_from_ss = 0  # 注： 为余额，是未偿还额
                shortqty_from_equity_compensation = 0  # 注： 是余额
                cash_from_ss_in_dict_secliability = 0
                list_fields_shortqty_from_ss = ['剩余数量']
                list_fields_shortqty_from_equity_compensation = ['权益补偿数量']  # 权益补偿数量，来自于股票分红，zhaos_tdx中该值为余额
                list_fields_ss_avgprice = ['卖均价']
                list_fields_cash_from_short_selling = ['融券卖出成本']  # CashFromShortSelling

                for dict_secliability in list_dicts_secliability:
                    secid = None
                    secidsrc = None
                    symbol = None
                    longqty = 0
                    for field_secid in list_fields_secid:
                        if field_secid in dict_secliability:
                            secid = str(dict_secliability[field_secid])

                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_secliability:
                            shareholder_acctid = dict_secliability[field_shareholder_acctid]
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_secliability:
                            exchange = dict_secliability[field_exchange]
                            dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE',
                                                      '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                                                      '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                                                      '00': 'SZSE', '10': 'SSE',
                                                      '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                                                      '上海A股': 'SSE', '深圳A股': 'SZSE',
                                                      '1': 'SSE', '2': 'SZSE',
                                                      }
                            secidsrc = dict_exchange2secidsrc[exchange]
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_secliability:
                            symbol = str(dict_secliability[field_symbol])

                    for field_shortqty_from_ss in list_fields_shortqty_from_ss:
                        if field_shortqty_from_ss in dict_secliability:
                            shortqty_from_ss = float(dict_secliability[field_shortqty_from_ss])

                    for field_shortqty_from_equity_compensation in list_fields_shortqty_from_equity_compensation:
                        if field_shortqty_from_equity_compensation in dict_secliability:
                            shortqty_from_equity_compensation = float(
                                dict_secliability[field_shortqty_from_equity_compensation])
                    shortqty = shortqty_from_ss + shortqty_from_equity_compensation

                    for field_ss_avgprice in list_fields_ss_avgprice:
                        if field_ss_avgprice in dict_secliability:
                            ss_avgprice = float(dict_secliability[field_ss_avgprice])
                            cash_from_ss_in_dict_secliability = shortqty_from_ss * ss_avgprice

                    for field_cash_from_short_selling in list_fields_cash_from_short_selling:
                        if field_cash_from_short_selling in dict_secliability:
                            cash_from_ss_in_dict_secliability = float(dict_secliability[field_cash_from_short_selling])

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = self.get_mingshi_sectype_from_code(windcode)
                    if sectype == 'IrrelevantItem':
                        continue
                    close = dict_wcode2close[windcode]
                    longamt = close * longqty
                    shortamt = close * shortqty
                    netamt = longamt - shortamt
                    dict_secliability_fmtted = {
                        'DataDate': self.str_today,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'LongAmt': longamt,
                        'ShortAmt': shortamt,
                        'NetAmt': netamt,
                        'CashFromShortSelling': cash_from_ss_in_dict_secliability,
                        'OTCContractUnitMarketValue': None,
                        'LiabilityType': (lambda x: 'Securities Liability' if x > 0 else None)(shortamt),
                        'Liability': shortamt,
                        'LiabilityQty': (lambda x: x if x else None)(shortqty),  # 融券数量
                        'LiabilityAmt': None,  # 融资数量
                        'InterestRate': None,
                        'DatedDate': None,
                        'UnderlyingSecurityID': None,
                        'UnderlyingSecurityIDSource': None,
                        'UnderlyingSecurityType': None,
                        'UnderlyingSymbol': None,
                        'UnderlyingQty': None,
                        'UnderlyingAmt': 0,
                        'UnderlyingClose': None,
                        'UnderlyingStartValue_vec': None,
                        'Note': None
                    }
                    list_dicts_holding_fmtted.append(dict_secliability_fmtted)

                # 1.2 patchdata
                # patchdata 逻辑： 是增量补充，而不是余额覆盖
                #   1. 检验是否有patchmark，有则进入patch算法，无则跳过。
                #   2. 格式化patch数据
                #   3. 将 patch data添加至holding中
                #   3. 检查capital中各字段是否为NoneType，是则使用由holding_patchdata中推算出来的值，否则使用当前值（capital）。
                #   4. 将rawdata 与 patchdata 相加，得到patched data。

                list_dicts_holding_patchdata_fmtted = []

                underlying_net_exposure = 0
                cash_from_ss_in_patch_data = 0
                if patchmark:
                    list_dicts_holding_patchdata = list(self.db_trddata['manually_patchdata_holding'].find(
                        {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}
                    ))
                    # todo 需改进自定义标的持仓价格和持仓金额的情况（eg.场外非标, 下层基金）
                    for dict_holding_patchdata in list_dicts_holding_patchdata:
                        if 'CashFromShortSelling' in dict_holding_patchdata:
                            cash_from_ss_in_patch_data += dict_holding_patchdata['CashFromShortSelling']
                        else:
                            cash_from_ss_in_patch_data += 0
                        underlying_sectype = dict_holding_patchdata['UnderlyingSecurityType']
                        if underlying_sectype in ['CS', 'ETF', 'INDEX']:
                            underlying_net_exposure_delta = dict_holding_patchdata['UnderlyingAmt']
                            underlying_net_exposure += underlying_net_exposure_delta
                        if underlying_sectype in ['Index Future']:
                            underlying_secid = dict_holding_patchdata['UnderlyingSecurityID']
                            underlying_index_future = underlying_secid[:2]
                            dict_index_future2index_spot_wcode = {'IC': '000905.SH', 'IF': '000300.SH',
                                                                  'IH': '000016.SH'}
                            close = dict_wcode2close[dict_index_future2index_spot_wcode[underlying_index_future]]
                            underlyingqty = dict_holding_patchdata['UnderlyingQty']
                            dict_index_future2multiplier = {'IC': 200, 'IF': 300, 'IH': 300}
                            multiplier = dict_index_future2multiplier[underlying_index_future]
                            underlying_net_exposure_delta = underlyingqty * close * multiplier
                            underlying_net_exposure += underlying_net_exposure_delta
                        list_dicts_holding_patchdata_fmtted.append(dict_holding_patchdata)
                list_dicts_holding_fmtted_patched = list_dicts_holding_fmtted + list_dicts_holding_patchdata_fmtted
                for dict_holding_fmtted_patched in list_dicts_holding_fmtted_patched:
                    prdcode = dict_holding_fmtted_patched['AcctIDByMXZ'].split('_')[0]
                    dict_holding_fmtted_patched['PrdCode'] = prdcode
                self.db_trddata['formatted_holding'].delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz})
                if list_dicts_holding_fmtted_patched:
                    self.db_trddata['formatted_holding'].insert_many(list_dicts_holding_fmtted_patched)

                underlying_exposure_long = 0
                underlying_exposure_short = 0
                if underlying_net_exposure >= 0:
                    underlying_exposure_long = underlying_net_exposure
                else:
                    underlying_exposure_short = - underlying_net_exposure

                # 2.整理b/s: 出于稳健性考量，股票市值由持仓数据计算得出: holding data + raw data + patch data
                # 用现金代替可用，重点计算
                # Cash, CashEquivalent, CompositeLongAmt, Asset, ShortAmt, Liability, ApproximateNetAsset
                # patch data采用余额覆盖模式。（holding data采用增量添加模式）

                # 2.1 holding data
                stock_longamt = 0
                etf_longamt = 0
                ce_longamt = 0
                swap_longamt = 0
                stock_shortamt = 0
                etf_shortamt = 0
                flt_capital_debt = 0

                if accttype in ['c', 'f', 'o']:
                    cash_from_ss_by_acctidbymxz = None
                elif accttype in ['m']:
                    cash_from_ss_by_acctidbymxz = 0
                    for dict_holding_fmtted_patched in list_dicts_holding_fmtted_patched:
                        cash_from_ss_by_acctidbymxz += dict_holding_fmtted_patched['CashFromShortSelling']
                else:
                    raise ValueError('Unknown accttype.')

                df_holding_fmtted_patched = pd.DataFrame(list_dicts_holding_fmtted_patched)
                if df_holding_fmtted_patched.empty:
                    df_holding_fmtted_patched = pd.DataFrame(
                        columns=['DataDate', 'AcctIDByMXZ', 'CashFromSS', 'SecurityID', 'SecurityType',
                                 'Symbol', 'SecurityIDSource',
                                 'LongQty', 'ShortQty', 'LongAmt', 'ShortAmt', 'NetAmt', 'CashFromShortSelling',
                                 'OTCContractUnitMarketValue', 'LiabilityType', 'Liability', 'LiabilityQty',
                                 'LiabilityAmt',
                                 'InterestRate', 'DatedDate', 'UnderlyingSecurityID', 'UnderlyingSecurityIDSource',
                                 'UnderlyingSecurityType', 'UnderlyingSymbol', 'UnderlyingQty', 'UnderlyingAmt',
                                 'UnderlyingClose', 'UnderlyingStartValue_vec', 'Note'])
                else:
                    df_holding_amt_sum_by_sectype = df_holding_fmtted_patched.groupby(by='SecurityType').sum()
                    dict_amt2dict_sectype2amt = df_holding_amt_sum_by_sectype.to_dict()
                    if 'LongAmt' in dict_amt2dict_sectype2amt:
                        dict_sectype2longamt = dict_amt2dict_sectype2amt['LongAmt']
                        if 'CS' in dict_sectype2longamt:
                            stock_longamt = dict_sectype2longamt['CS']
                        if 'ETF' in dict_sectype2longamt:
                            etf_longamt = dict_sectype2longamt['ETF']
                        if 'CE' in dict_sectype2longamt:
                            ce_longamt = dict_sectype2longamt['CE']
                        if 'SWAP' in dict_sectype2longamt:
                            swap_longamt = dict_sectype2longamt['SWAP']
                    if 'ShortAmt' in dict_amt2dict_sectype2amt:
                        dict_sectype2shortamt = dict_amt2dict_sectype2amt['ShortAmt']
                        if 'CS' in dict_sectype2shortamt:
                            stock_shortamt = dict_sectype2shortamt['CS']
                        if 'ETF' in dict_sectype2shortamt:
                            etf_shortamt = dict_sectype2shortamt['ETF']

                # 2.2 求cash
                dict_capital = self.db_trddata['manually_rawdata_capital'].find_one(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}, {'_id': 0}
                )
                if dict_capital is None:
                    dict_capital = {}
                list_fields_af = ['可用', 'A股可用', '可用数', '现金资产', '可用金额', '资金可用金', '可用余额', 'T+1指令可用金额',
                                  'enable_balance']
                list_fields_ttasset = ['总资产', '资产', '总 资 产', '实时总资产', '单元总资产', '资产总额', '账户总资产',
                                       '担保资产', 'asset_balance', 'assure_asset']
                flt_ttasset = None
                flt_cash = None

                # 分两种情况： 1. cash acct: 至少要有cash 2. margin acct: 至少要有ttasset
                if accttype in ['c']:
                    for field_af in list_fields_af:
                        if field_af in dict_capital:
                            flt_cash = float(dict_capital[field_af])
                        else:
                            if patchmark:
                                dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}, {'_id': 0}
                                ))
                                if dict_patchdata_capital:
                                    if 'Cash' in dict_patchdata_capital:
                                        flt_cash = dict_patchdata_capital['Cash']
                                    if 'CapitalDebt' in dict_patchdata_capital:
                                        flt_capital_debt = dict_patchdata_capital['CapitalDebt']

                elif accttype == 'm':
                    for field_ttasset in list_fields_ttasset:
                        if field_ttasset in dict_capital:
                            flt_ttasset = float(dict_capital[field_ttasset])
                    if patchmark:
                        dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                            {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}, {'_id': 0}
                        ))
                        if dict_patchdata_capital:
                            if 'TotalAsset' in dict_patchdata_capital:
                                if dict_patchdata_capital['TotalAsset']:
                                    flt_ttasset = dict_patchdata_capital['TotalAsset']
                            if 'CapitalDebt' in dict_patchdata_capital:
                                flt_capital_debt = dict_patchdata_capital['CapitalDebt']

                    flt_cash = flt_ttasset - stock_longamt - etf_longamt - ce_longamt

                elif accttype == 'o':
                    dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                        {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}, {'_id': 0}
                    ))
                    if dict_patchdata_capital:
                        if 'Cash' in dict_patchdata_capital:
                            flt_cash = dict_patchdata_capital['Cash']
                        if 'CapitalDebt' in dict_patchdata_capital:
                            flt_capital_debt = dict_patchdata_capital['CapitalDebt']
                else:
                    raise ValueError('Unknown accttype')

                # 2.3 cash equivalent: ce_longamt
                flt_ce = ce_longamt

                # 2.4 etf
                flt_etf_long_amt = etf_longamt

                # 2.4 CompositeLongAmt
                flt_composite_long_amt = stock_longamt

                # 2.5 SwapAmt
                # 更新：大于0的进交易性金融资产，为资产端项目；小于0的进交易性金融负债，为负债端项目
                flt_swap_amt2asset = 0
                if swap_longamt > 0:
                    flt_swap_amt2asset = swap_longamt
                flt_swap_amt2liability = 0
                if swap_longamt < 0:
                    flt_swap_amt2liability = abs(swap_longamt)

                # 2.5 Asset
                flt_ttasset = flt_cash + flt_ce + flt_etf_long_amt + flt_composite_long_amt + flt_swap_amt2asset

                # 2.6 etf_shortamt
                flt_etf_short_amt = etf_shortamt

                # 2.7 stock_shortamt
                flt_composite_short_amt = stock_shortamt

                # 2.8 liability
                # liability = 融券负债（利息+本金）+ 融资负债（利息+本金）+ 场外合约形成的负债（交易性金融负债）
                if flt_capital_debt is None:
                    flt_capital_debt = 0
                flt_liability = (
                        float(df_holding_fmtted_patched['Liability'].sum()) + flt_capital_debt + flt_swap_amt2liability
                )

                # 2.9 net_asset
                flt_approximate_na = flt_ttasset - flt_liability

                # 2.10 读取patchdata对rawdata进行修正: 余额覆盖模式
                if patchmark:
                    dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                        {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_today}, {'_id': 0}
                    ))
                    if dict_patchdata_capital:
                        if 'Cash' in dict_patchdata_capital:
                            if dict_patchdata_capital['Cash']:
                                flt_cash = float(dict_patchdata_capital['Cash'])
                        if 'CashEquivalent' in dict_patchdata_capital:
                            if dict_patchdata_capital['CashEquivalent']:
                                flt_ce = float(dict_patchdata_capital['CashEquivalent'])
                        if 'ETFLongAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['ETFLongAmt']:
                                flt_etf_long_amt = float(dict_patchdata_capital['ETFLongAmt'])
                        if 'CompositeLongAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['CompositeLongAmt']:
                                flt_composite_long_amt = float(dict_patchdata_capital['CompositeLongAmt'])
                        if 'TotalAsset' in dict_patchdata_capital:
                            if dict_patchdata_capital['TotalAsset']:
                                flt_ttasset = float(dict_patchdata_capital['TotalAsset'])
                        if 'ETFShortAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['ETFShortAmt']:
                                flt_etf_short_amt = float(dict_patchdata_capital['ETFShortAmt'])
                        if 'CompositeShortAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['CompositeShortAmt']:
                                flt_composite_short_amt = float(dict_patchdata_capital['CompositeShortAmt'])
                        if 'Liability' in dict_patchdata_capital:
                            if dict_patchdata_capital['Liability']:
                                flt_liability = float(dict_patchdata_capital['Liability'])
                        if 'ApproximateNetAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['ApproximateNetAmt']:
                                flt_approximate_na = float(dict_patchdata_capital['ApproximateNetAmt'])
                dict_bs = {
                    'DataDate': self.str_today,
                    'PrdCode': prdcode,
                    'AcctIDByMXZ': acctidbymxz,
                    'AcctType': accttype,
                    'Cash': flt_cash,
                    'CashEquivalent': flt_ce,
                    'CashFromShortSelling': cash_from_ss_by_acctidbymxz,
                    'ETFLongAmt': flt_etf_long_amt,
                    'CompositeLongAmt': flt_composite_long_amt,
                    'AssetFromSwap': flt_swap_amt2asset,
                    'TotalAsset': flt_ttasset,
                    'CapitalDebt': flt_capital_debt,
                    'ETFShortAmt': flt_etf_short_amt,
                    'CompositeShortAmt': flt_composite_short_amt,
                    'LiabilityFromSwap': flt_swap_amt2liability,
                    'Liability': flt_liability,
                    'ApproximateNetAsset': flt_approximate_na,
                }
                self.db_trddata['b/s_by_acctidbymxz'].delete_many({'DataDate': self.str_today,
                                                                   'AcctIDByMXZ': acctidbymxz})
                list_dict_bs = [dict_bs]
                if dict_bs:
                    self.db_trddata['b/s_by_acctidbymxz'].insert_many(list_dict_bs)

                exposure_long_amt = flt_etf_long_amt + flt_composite_long_amt + underlying_exposure_long
                exposure_short_amt = flt_etf_short_amt + flt_composite_short_amt + underlying_exposure_short
                exposure_net_amt = exposure_long_amt - exposure_short_amt

            elif accttype in ['f']:
                # 按acctidbymxz exposure数据
                list_dicts_holding_future = list(
                    self.db_trddata['future_api_holding'].find({'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz})
                )
                list_dicts_holding_future_exposure_draft = []
                for dict_holding_future in list_dicts_holding_future:
                    secid = dict_holding_future['instrument_id']
                    secid_first_part = secid[:-4]
                    dict_future2spot_windcode = {'IC': '000905.SH', 'IH': '000016.SH', 'IF': '000300.SH'}
                    windcode = dict_future2spot_windcode[secid_first_part]
                    close = dict_wcode2close[windcode]  # spot close
                    qty = dict_holding_future['position']
                    direction = dict_holding_future['direction']
                    future_long_qty = 0
                    future_short_qty = 0
                    future_long_amt = 0
                    future_short_amt = 0

                    if direction == 'buy':
                        future_long_qty = qty
                        future_long_amt = close * future_long_qty * self.dict_future2multiplier[secid_first_part]
                    elif direction == 'sell':
                        future_short_qty = qty
                        future_short_amt = close * future_short_qty * self.dict_future2multiplier[secid_first_part]
                    else:
                        raise ValueError('Unknown direction in future respond.')
                    future_net_qty = future_long_qty - future_short_qty
                    future_net_amt = future_long_amt - future_short_amt
                    dict_holding_future_exposure_draft = {
                        'SecurityID': secid,
                        'direction': direction,
                        'position': qty,
                        'LongQty': future_long_qty,
                        'ShortQty': future_short_qty,
                        'NetQty': future_net_qty,
                        'LongAmt': future_long_amt,
                        'ShortAmt': future_short_amt,
                        'NetAmt': future_net_amt,
                    }
                    list_dicts_holding_future_exposure_draft.append(dict_holding_future_exposure_draft)
                if list_dicts_holding_future_exposure_draft:
                    """一个账户的全部品种风险暴露（对IC提供的还是IH提供的未作区分）"""
                    df_holding_future_exposure_draft = pd.DataFrame(list_dicts_holding_future_exposure_draft)
                    exposure_long_amt = float(df_holding_future_exposure_draft['LongAmt'].sum())
                    exposure_short_amt = float(df_holding_future_exposure_draft['ShortAmt'].sum())
                    exposure_net_amt = exposure_long_amt - exposure_short_amt
                else:
                    exposure_long_amt = 0
                    exposure_short_amt = 0
                    exposure_net_amt = 0
                dict_capital_future = (
                    self.db_trddata['future_api_capital'].find_one(
                        {'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz}
                    )
                )
                flt_approximate_na = dict_capital_future['DYNAMICBALANCE']
                dict_future_bs = {
                    'DataDate': self.str_today,
                    'AcctIDByMXZ': acctidbymxz,
                    'AcctType': 'f',
                    'ApproximateNetAsset': flt_approximate_na,
                    'Cash': flt_approximate_na,
                    'CashEquivalent': 0,
                    'CompositeLongAmt': 0,
                    'CompositeShortAmt': 0,
                    'ETFLongAmt': 0,
                    'ETFShortAmt': 0,
                    'Liability': 0,
                    'PrdCode': prdcode,
                    'SwapLongAmt': 0,
                    'TotalAsset': flt_approximate_na,
                }
                self.db_trddata['b/s_by_acctidbymxz'].delete_many({'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz})
                if dict_future_bs:
                    self.db_trddata['b/s_by_acctidbymxz'].insert_one(dict_future_bs)

            else:
                raise ValueError('Unknown account type in basic account info.')

            dict_exposure_analysis = {
                'DataDate': self.str_today,
                'AcctIDByMXZ': acctidbymxz,
                'PrdCode': prdcode,
                'LongExposure': exposure_long_amt,
                'ShortExposure': exposure_short_amt,
                'NetExposure': exposure_net_amt,
                'ApproximateNetAsset': flt_approximate_na,
            }
            self.db_trddata['exposure_analysis_by_acctidbymxz'].delete_many({'DataDate': self.str_today,
                                                                             'AcctIDByMXZ': acctidbymxz})
            if dict_exposure_analysis:
                self.db_trddata['exposure_analysis_by_acctidbymxz'].insert_one(dict_exposure_analysis)
        print('Update capital and holding formatted by internal style finished.')

    def update_bs_by_prdcode_and_exposure_analysis_by_prdcode(self):
        list_dicts_bs_by_acct = list(
            self.db_trddata['b/s_by_acctidbymxz'].find({'DataDate': self.str_today}, {'_id': 0})
        )
        df_bs_by_acct = pd.DataFrame(list_dicts_bs_by_acct)
        df_bs_by_prdcode = df_bs_by_acct.groupby(by='PrdCode').sum().reset_index()
        df_bs_by_prdcode['DataDate'] = self.str_today
        list_dicts_bs_by_prdcode = df_bs_by_prdcode.to_dict('records')
        self.db_trddata['b/s_by_prdcode'].delete_many({'DataDate': self.str_today})
        if list_dicts_bs_by_prdcode:
            self.db_trddata['b/s_by_prdcode'].insert_many(list_dicts_bs_by_prdcode)

        list_dicts_exposure_analysis_by_acctidbymxz = list(
            self.db_trddata['exposure_analysis_by_acctidbymxz'].find({'DataDate': self.str_today}, {'_id': 0})
        )
        df_exposure_analysis_by_acctidbymxz = pd.DataFrame(list_dicts_exposure_analysis_by_acctidbymxz)
        df_exposure_analysis_by_prdcode = df_exposure_analysis_by_acctidbymxz.groupby(by='PrdCode').sum().reset_index()
        df_exposure_analysis_by_prdcode['DataDate'] = self.str_today
        df_exposure_analysis_by_prdcode['NetExposure(%)'] = (
                df_exposure_analysis_by_prdcode['NetExposure'] / df_exposure_analysis_by_prdcode['ApproximateNetAsset']
        )

        list_dicts_exposure_analysis_by_prdcode = df_exposure_analysis_by_prdcode.to_dict('records')
        for dict_exposure_analysis_by_prdcode in list_dicts_exposure_analysis_by_prdcode:
            prdcode = dict_exposure_analysis_by_prdcode['PrdCode']
            dict_prdinfo = self.col_prdinfo.find_one({'DataDate': self.str_today, 'PrdCode': prdcode})
            if dict_prdinfo is None:  # todo 需要再考虑
                continue
            dict_strategies_allocation = dict_prdinfo['StrategiesAllocation']
            prd_approximate_na = dict_exposure_analysis_by_prdcode['ApproximateNetAsset']
            net_exposure_amt = dict_exposure_analysis_by_prdcode['NetExposure']
            net_exposure_pct_tgt = (
                    0.99 * dict_strategies_allocation['EI'] + 0 * dict_strategies_allocation['MN']
            )
            net_exposure_amt_tgt = net_exposure_pct_tgt * prd_approximate_na
            dif_net_exposure_amt = net_exposure_amt - net_exposure_amt_tgt
            dict_exposure_analysis_by_prdcode['NetExposure(%)Tgt'] = net_exposure_pct_tgt
            dict_exposure_analysis_by_prdcode['NetExposureTgt'] = net_exposure_amt_tgt
            dict_exposure_analysis_by_prdcode['NetExposureDif'] = dif_net_exposure_amt

        self.db_trddata['exposure_analysis_by_prdcode'].delete_many({'DataDate': self.str_today})
        if list_dicts_exposure_analysis_by_prdcode:
            self.db_trddata['exposure_analysis_by_prdcode'].insert_many(list_dicts_exposure_analysis_by_prdcode)
        print('Update b/s by prdcode and exposure analysis by prdcode finished')

    def update_faccts_holding_aggr(self):
        """支持多品种"""
        list_dicts_acctinfo_faccts = list(
            self.col_acctinfo.find({'AcctType': 'f', 'RptMark': 1, 'DataDate': self.str_today})
        )
        list_dicts_pstaggr_facct = []
        set_prdcodes = set()
        for dict_acctinfo_facct in list_dicts_acctinfo_faccts:
            acctidbymxz = dict_acctinfo_facct['AcctIDByMXZ']
            prdcode = acctidbymxz.split('_')[0]
            set_prdcodes.add(prdcode)
            list_dicts_future_api_holding = list(self.db_trddata['future_api_holding'].find(
                {'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz}
            ))
            set_secids_first_part = set()
            for dict_future_api_holding in list_dicts_future_api_holding:
                secid = dict_future_api_holding['instrument_id']
                secid_first_part = secid[:-4]
                set_secids_first_part.add(secid_first_part)
            list_secids_first_part = list(set_secids_first_part)
            list_secids_first_part.sort()

            dict_secid_first_part2vecqty = {}
            for secid_first_part in list_secids_first_part:
                vecqty = 0
                for dict_future_api_holding in list_dicts_future_api_holding:
                    direction = dict_future_api_holding['direction']
                    position = dict_future_api_holding['position']
                    secid_first_part_in_api_holding = dict_future_api_holding['instrument_id'][:-4]
                    if direction == 'buy':
                        vecqty_delta = position
                    elif direction == 'sell':
                        vecqty_delta = - position
                    else:
                        raise ValueError('Unknown direction.')
                    if secid_first_part == secid_first_part_in_api_holding:
                        vecqty += vecqty_delta
                dict_secid_first_part2vecqty.update({secid_first_part: vecqty})

            dict_pstaggr_facct = {
                'DataDate': self.str_today,
                'AcctIDByMXZ': acctidbymxz,
                'PrdCode': prdcode,
                'holding_aggr_by_secid_first_part': dict_secid_first_part2vecqty,
            }
            list_dicts_pstaggr_facct.append(dict_pstaggr_facct)

        self.db_trddata['facct_holding_aggr_by_acctidbymxz'].delete_many({'DataDate': self.str_today})
        if list_dicts_pstaggr_facct:
            self.db_trddata['facct_holding_aggr_by_acctidbymxz'].insert_many(list_dicts_pstaggr_facct)

        list_prdcodes = list(set_prdcodes)
        list_prdcodes.sort()
        list_facct_holding_aggr_by_prdcode = []
        for prdcode in list_prdcodes:
            set_secids_first_part_by_prdcode = set()
            for dict_pstaggr_facct in list_dicts_pstaggr_facct:
                if prdcode == dict_pstaggr_facct['PrdCode']:
                    list_secids_first_part_by_prdcode = dict_pstaggr_facct['holding_aggr_by_secid_first_part'].keys()
                    for _ in list_secids_first_part_by_prdcode:
                        set_secids_first_part_by_prdcode.add(_)
            list_secids_first_part_by_prdcode = list(set_secids_first_part_by_prdcode)
            list_secids_first_part_by_prdcode.sort()
            dict_pstaggr_facct_by_prdcode = {}
            for secid_first_part_by_prdcode in list_secids_first_part_by_prdcode:
                vecqty = 0
                for dict_pstaggr_facct in list_dicts_pstaggr_facct:
                    if prdcode == dict_pstaggr_facct['PrdCode']:
                        if secid_first_part_by_prdcode in dict_pstaggr_facct['holding_aggr_by_secid_first_part']:
                            vecqty_delta = (
                                dict_pstaggr_facct['holding_aggr_by_secid_first_part'][secid_first_part_by_prdcode]
                            )
                        else:
                            vecqty_delta = 0
                        vecqty += vecqty_delta
                dict_pstaggr_facct_by_prdcode.update({secid_first_part_by_prdcode: vecqty})
            dict_facct_holding_aggr_by_prdcode = {
                'DataDate': self.str_today,
                'PrdCode': prdcode,
                'holding_aggr_by_prdcode': dict_pstaggr_facct_by_prdcode,
            }
            list_facct_holding_aggr_by_prdcode.append(dict_facct_holding_aggr_by_prdcode)

        self.db_trddata['facct_holding_aggr_by_prdcode'].delete_many({'DataDate': self.str_today})
        if list_facct_holding_aggr_by_prdcode:
            self.db_trddata['facct_holding_aggr_by_prdcode'].insert_many(list_facct_holding_aggr_by_prdcode)
        print('Update holding_aggr_by_acctidbymxz and facct_holding_aggr_by_prdcode finished')

    def update_col_tgtna_by_prdcode(self):
        """
        这个表处理所有产品的目标净资产。
        算法：
            1. 读取源数据中的净资产
            2. 读取生效的划款数据，进行汇总
            3. 读取指定的净资产
            4. 计算预算用目标净资产

        todo
            1. 可拓展各渠道的净资产
        """
        list_dicts_tgtna_by_prdcode = []
        list_dicts_prdinfo = self.col_prdinfo.find({'DataDate': self.str_today, 'RptMark': 1})
        for dict_prdinfo in list_dicts_prdinfo:
            prdcode = dict_prdinfo['PrdCode']
            dict_bs_by_prdcode = self.col_bs_by_prdcode.find_one(
                {'PrdCode': prdcode, 'DataDate': self.str_today}
            )
            approximate_na = dict_bs_by_prdcode['ApproximateNetAsset']
            list_dicts_manually_patchdata_dwitems = list(
                self.col_manually_patchdata_dwitems.find(
                    {'DataDate': self.str_today, 'PrdCode': prdcode}
                )
            )
            amt2dw_by_prdcode = 0
            for dict_manually_patchdata_dwitems in list_dicts_manually_patchdata_dwitems:
                dwbgt_netamt_estimated2dw = dict_manually_patchdata_dwitems['DWBGTNetAMTEstimated2DW']
                status = dict_manually_patchdata_dwitems['Status']
                if status in [2, 3]:
                    amt2dw_by_prdcode += dwbgt_netamt_estimated2dw

            dict_tgt_items = dict_prdinfo['TargetItems']
            designated_na = None
            if dict_tgt_items:
                if dict_tgt_items['NetAsset']:
                    designated_na = dict_tgt_items['NetAsset']

            if designated_na:
                tgtprdna = designated_na
            else:
                tgtprdna = approximate_na + amt2dw_by_prdcode

            dict_tgtna_by_prdcode = {
                'DataDate': self.str_today,
                'PrdCode': prdcode,
                'CurrentNAFromInternalData': approximate_na,
                'Amt2DW': amt2dw_by_prdcode,
                'TgtNA': tgtprdna,
            }
            list_dicts_tgtna_by_prdcode.append(dict_tgtna_by_prdcode)
        self.col_tgtna_by_prdcode.delete_many({'DataDate': self.str_today})
        if list_dicts_tgtna_by_prdcode:
            self.col_tgtna_by_prdcode.insert_many(list_dicts_tgtna_by_prdcode)

    def update_col_cpslongamt_from_sse_by_acctidbymxz_and_col_cpslongamt_from_sse_by_prdcode(self):
        list_dicts_cpslongamt_from_sse_by_acctidbymxz = []
        list_dicts_acctinfo = list(
            self.col_acctinfo.find({'DataDate': self.str_today, 'RptMark': 1, 'SpecialAccountMark': 0})
        )
        for dict_acctinfo in list_dicts_acctinfo:
            accttype = dict_acctinfo['AcctType']
            if accttype in ['c', 'm']:
                acctidbymxz = dict_acctinfo['AcctIDByMXZ']
                prdcode = dict_acctinfo['PrdCode']
                list_dicts_formatted_holding_by_acctidbymxz = list(
                    self.col_formatted_holding.find({'DataDate': self.str_today, 'AcctIDByMXZ': acctidbymxz})
                )
                cpslongamt_from_sse = 0
                for dict_formatted_holding_by_acctidbymxz in list_dicts_formatted_holding_by_acctidbymxz:
                    secidsrc = dict_formatted_holding_by_acctidbymxz['SecurityIDSource']
                    sectype = dict_formatted_holding_by_acctidbymxz['SecurityType']
                    if secidsrc in ['SSE'] and sectype in ['CS']:
                        cpslongamt_from_sse += dict_formatted_holding_by_acctidbymxz['LongAmt']
                dict_cpslongamt_from_sse_by_acctidbymxz = {
                    'DataDate': self.str_today,
                    'AcctIDByMXZ': acctidbymxz,
                    'PrdCode': prdcode,
                    'cpslongamt_from_sse': cpslongamt_from_sse
                }
                list_dicts_cpslongamt_from_sse_by_acctidbymxz.append(dict_cpslongamt_from_sse_by_acctidbymxz)

        self.col_cpslongamt_from_sse_by_acctidbymxz.delete_many({'DataDate': self.str_today})
        if list_dicts_cpslongamt_from_sse_by_acctidbymxz:
            self.col_cpslongamt_from_sse_by_acctidbymxz.insert_many(list_dicts_cpslongamt_from_sse_by_acctidbymxz)

        list_dicts_cpslongamt_from_sse_by_prdcode = []
        list_dicts_prdinfo = list(self.col_acctinfo.find({'DataDate': self.str_today, 'RptMark': 1}))
        set_prdcodes = set()
        for dict_prdinfo in list_dicts_prdinfo:
            prdcode = dict_prdinfo['PrdCode']
            set_prdcodes.add(prdcode)
        list_prdcodes = list(set_prdcodes)

        for prdcode in list_prdcodes:
            list_dicts_formatted_holding_by_acctidbymxz = list(
                self.col_formatted_holding.find({'DataDate': self.str_today, 'PrdCode': prdcode})
            )
            cpslongamt_from_sse = 0
            for dict_formatted_holding_by_acctidbymxz in list_dicts_formatted_holding_by_acctidbymxz:
                secidsrc = dict_formatted_holding_by_acctidbymxz['SecurityIDSource']
                sectype = dict_formatted_holding_by_acctidbymxz['SecurityType']
                if secidsrc in ['SSE'] and sectype in ['CS']:
                    cpslongamt_from_sse += dict_formatted_holding_by_acctidbymxz['LongAmt']
            dict_cpslongamt_from_sse_by_prdcode = {
                'DataDate': self.str_today,
                'PrdCode': prdcode,
                'cpslongamt_from_sse': cpslongamt_from_sse
            }
            list_dicts_cpslongamt_from_sse_by_prdcode.append(dict_cpslongamt_from_sse_by_prdcode)

        self.col_cpslongamt_from_sse_by_prdcode.delete_many({'DataDate': self.str_today})
        if list_dicts_cpslongamt_from_sse_by_prdcode:
            self.col_cpslongamt_from_sse_by_prdcode.insert_many(list_dicts_cpslongamt_from_sse_by_prdcode)
        print('Update cpslongamt from sse finished.')

    def run(self):
        print(f"Running time: {datetime.now().strftime('%Y%m%dT%H%M%S')}")
        self.update_trddata_f()
        self.update_faccts_holding_aggr()
        self.update_rawdata()
        self.update_manually_patchdata()
        self.update_formatted_holding_and_balance_sheet_and_exposure_analysis()
        self.update_bs_by_prdcode_and_exposure_analysis_by_prdcode()
        self.update_col_cpslongamt_from_sse_by_acctidbymxz_and_col_cpslongamt_from_sse_by_prdcode()
        self.update_col_tgtna_by_prdcode()
        self.update_na_allocation()
        print("Database preparation finished.")


if __name__ == '__main__':
    task = DBTradingData()
    task.run()














