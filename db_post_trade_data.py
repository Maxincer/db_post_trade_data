"""
Note:
    1. 输入数据的地址统一为七类, 空缺的位置为空:
        DataFilePath = [fund, holding, order, short_position, public_security_loan, private_security_loan, jgd]
        PostTradeDataFilePath = [fund, holding, order, short_position, public_security_loan, private_security_loan, jgd]
    2. TH改进算法: 读一次数据，减少冗余查询次数，大幅缩短运行时间。
        1. 对相同的 datafilepath 分组，读一次。basic info 里读取文件的地址一样归类，很多账户都在一个文件里，以避免内存的冗余占用，核心。
    3. 此次改动将从藕老师处得到的期货端数据直接作为raw data 入库， 对原来future 与 stock分开的情况进行合并处理

Abbr:
    1. rdct, raw_data_content_type: content type of raw data, 依据源数据内容划分的数据表名， 特指:
        [fund, holding, order, short_position, public_security_loan, private_security_loan, jgd]
    2. kqzj,  可取资金

Todo:
    1. post trade data, 程序 - 日期更改， 改两个文件: 1. 写入的脚本， 2.读取的脚本
    2. patch data 添加
    3. 期货数据清算后数据？
"""
import codecs
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

from openpyxl import load_workbook
import pandas as pd
from xlrd import open_workbook

from globals import Globals

# 设置log
logger_expo = logging.getLogger()
logger_expo.setLevel(logging.DEBUG)
fh = RotatingFileHandler('data/log/post_trade_data.log', mode='w', maxBytes=2*1024, backupCount=0)
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s - line:%(lineno)d - %(levelname)s: %(message)s'))
logger_expo.addHandler(fh)


class UpdatePostTradeData:
    def __init__(self):
        self.gl = Globals()
        df_acctinfo = pd.DataFrame(
            self.gl.col_acctinfo.find({'DataDate': self.gl.str_last_trddate, 'DataDownloadMark': 1}, {'_id': 0})
        )
        df_acctinfo = df_acctinfo.set_index('AcctIDByMXZ')
        self.dict_acctidbymxz2acctinfo = df_acctinfo.to_dict()
        self.list_warn = []
        self.dict_secid2secidsrc = {}  # 记录特殊证券代码与代码源的关系

    def read_rawdata_from_trdclient(self, fpath, sheet_name, data_source_type, accttype, dict_dldfilter2acctidbymxz):
        """
        从客户端下载数据，并进行初步清洗。为字符串格式。
        tdx倒出的txt文件有“五粮液错误”，使用xls格式的可解决

        已更新券商处理格式：
            华泰: hexin, txt, cash, margin, fund, holding
            国君: 富易, csv
            海通: ehtc, xlsx, cash, fund, holding
            申宏: alphabee, txt
            建投: alphabee, txt
            中信: tdx, txt, vip, cash, fund, holding,
            民生: tdx, txt
            华福: tdx, txt

        :param fpath: 原始数据路径
        :param dict_dldfilter2acctidbymxz: dict, {datadld_filter: acctidbymxz}. 逻辑为由地址出发寻找帐号
        :param accttype: c: cash, m: margin, f: future
        :param sheet_name: ['fund', 'holding', 'order', 'short_position']
        :param data_source_type:

        :return: list: 由dict rec组成的list
        """

        list_ret = []
        if sheet_name == 'fund':
            dict_rec_fund = {}
            if data_source_type in ['huat_hx', 'hait_hx', 'zhes_hx', 'tf_hx', 'db_hx', 'wk_hx'] and accttype == 'c':
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()[0:6]
                    for dataline in list_datalines:
                        list_data = dataline.strip().split(b'\t')
                        for data in list_data:
                            list_recdata = data.strip().decode('gbk').split('：')
                            dict_rec_fund[list_recdata[0].strip()] = list_recdata[1].strip()
                        if dict_rec_fund:
                            list_ret.append(dict_rec_fund)

            elif data_source_type in ['yh_hx'] and accttype in ['c']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[5].decode('gbk').split()
                    list_values = list_datalines[6].decode('gbk').split()
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['yh_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_fund = df_read.to_dict('records')[0]
                if dict_rec_fund:
                    list_ret.append(dict_rec_fund)

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
                            dict_rec_fund[list_recdata[0].strip()] = \
                                (lambda x: x if x.strip() in ['人民币'] else list_recdata[1].strip())(list_recdata[1])
                        if dict_rec_fund:
                            list_ret.append(dict_rec_fund)

            elif data_source_type in ['gtja_fy'] and accttype in ['c', 'm']:
                wb = open_workbook(fpath, encoding_override='gbk')
                ws = wb.sheet_by_index(0)
                list_keys = ws.row_values(5)
                list_values = ws.row_values(6)
                list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['hait_ehtc'] and accttype == 'c':
                df_read = pd.read_excel(fpath, skiprows=1, nrows=1)
                dict_rec_fund = df_read.to_dict('records')[0]
                if dict_rec_fund:
                    list_ret.append(dict_rec_fund)

            elif data_source_type in ['hait_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_fund = df_read.to_dict('records')[0]
                if dict_rec_fund:
                    list_ret.append(dict_rec_fund)

            elif data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    dataline = list_datalines[0][8:]
                    list_recdata = dataline.strip().decode('gbk').split()
                    for recdata in list_recdata:
                        list_recdata = recdata.split(':')
                        list_ret.append({list_recdata[0]: list_recdata[1]})

            elif (data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx']
                  and accttype in ['c', 'm']):
                # 已改为xls版本，避免'五粮液错误'
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    list_values = list_datalines[1].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['zxjt_alphabee', 'swhy_alphabee'] and accttype in ['c', 'm']:
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_last_trddate)
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split()
                    list_values = list_datalines[1].decode('gbk').split()
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['swhy_alphabee_dbf2csv', 'ax_custom']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split(',')
                    list_values = list_datalines[1].decode('gbk').split(',')
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['patch']:
                pass

            elif data_source_type in ['zx_wealthcats']:
                fpath = (
                    fpath
                        .replace('<YYYY-MM-DD>', self.gl.dt_today.strftime('%Y-%m-%d'))
                        .replace('<YYYYMMDD>', self.gl.str_today)
                )
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath)
                    else:
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s' % fpath)
                        else:
                            list_keys = list_datalines[0].strip().split(',')

                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户'] in dict_dldfilter2acctidbymxz:  # 中信下载的文件中，账户字段是dlddatafilter
                                dict_fund_wealthcats['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_fund_wealthcats['账户']]
                                list_ret.append(dict_fund_wealthcats)

            elif data_source_type in ['db_wealthcats']:
                fpath = fpath.replace('<YYYY-MM-DD>', self.gl.str_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户'] in dict_dldfilter2acctidbymxz:
                                dict_fund_wealthcats['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_fund_wealthcats['账户']]
                                list_ret.append(dict_fund_wealthcats)

            elif data_source_type in ['ax_jzpb']:
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with open(fpath, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['账户编号'] in dict_dldfilter2acctidbymxz:
                                dict_fund['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_fund['账户编号']]
                                list_ret.append(dict_fund)

            elif data_source_type in [
                'zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb',
                'swhy_xtpb', 'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb', 'dh_xtqmt'
            ]:
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账号'] in dict_dldfilter2acctidbymxz:
                                dict_fund['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_fund['资金账号']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['hait_ehfz_api']:   # 有改动
                for acctidbybroker in dict_dldfilter2acctidbymxz:
                    fpath_ = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                    with codecs.open(fpath_, 'rb', 'gbk') as f:
                        list_datalines = f.readlines()
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s' % fpath_)
                        else:
                            list_keys = list_datalines[0].strip().split(',')
                        for dataline in list_datalines[1:]:
                            list_values = dataline.strip().split(',')
                            if len(list_values) == len(list_keys):
                                dict_fund = dict(zip(list_keys, list_values))
                                dict_fund['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]  # fpath里自带交易账户， dict_dldfilter2acctidbymxz仅一个
                                list_ret.append(dict_fund)

            elif data_source_type in ['huat_matic_tsi']:    # 有改动
                for acctidbybroker in dict_dldfilter2acctidbymxz:
                    fpath_ = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                    with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                        list_datalines = f.readlines()
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s'%fpath_)
                        else:
                            list_keys = list_datalines[0].strip().split(',')
                        for dataline in list_datalines[1:]:
                            list_values = dataline.strip().split(',')
                            if len(list_values) == len(list_keys):
                                dict_fund = dict(zip(list_keys, list_values))
                                if dict_fund['fund_account'] == acctidbybroker:
                                    dict_fund['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]
                                    list_ret.append(dict_fund)

            elif data_source_type in ['gy_htpb', 'gs_htpb', 'gj_htpb']:    # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账户'] in dict_dldfilter2acctidbymxz:
                                dict_fund['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_fund['资金账户']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['单元序号'] in dict_dldfilter2acctidbymxz:
                                dict_fund['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_fund['单元序号']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['yh_apama'] and accttype == 'c':  # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '币种', '可用余额', '可取金额', '冻结金额', '总资产', '证券市值', '资金资产']
                    for dataline in list_datalines:
                        dataline = dataline.strip('\n')
                        split_line = dataline.split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind+1:])   # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            dict_fund['AcctIDByMXZ'] = list(dict_dldfilter2acctidbymxz.values())[0]  # fpath里自带交易账户， dict_dldfilter2acctidbymxz仅一个
                            list_ret.append(dict_fund)
                        else:
                            logger_expo.warning('strange fund keys of yh_apama %s' % fpath)

            elif data_source_type in ['yh_apama'] and accttype == 'm':
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '币种', '可用余额', '可取金额', '冻结金额', '总资产', '证券市值',
                                 '资金资产', '总负债', '融资负债', '融券负债', '融资息费', '融券息费', '融资可用额度',
                                 '融券可用额度', '担保证券市值', '维持担保比例', '实时担保比例']
                    for dataline in list_datalines:
                        dataline = dataline.strip('\n')
                        split_line = dataline.split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind + 1:])  # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            dict_fund['AcctIDByMXZ'] = list(dict_dldfilter2acctidbymxz.values())[0]  # fpath里自带交易账户， dict_dldfilter2acctidbymxz仅一个
                            list_ret.append(dict_fund)
                        else:
                            logger_expo.warning('strange fund key of yh_apama %s'%fpath)

            elif data_source_type in ['gf_tyt']:
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['projectid'] in dict_dldfilter2acctidbymxz:
                                dict_fund['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_fund['projectid']]
                                list_ret.append(dict_fund)

            else:
                e = f'Field data_source_type:{data_source_type} in {sheet_name} not exist in basic info!'
                if e not in self.list_warn:
                    self.list_warn.append(e)
                    logger_expo.error(e)

        elif sheet_name == 'holding':
            if data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
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

            elif data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx'] and accttype in ['c',
                                                                                                              'm']:
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
                    acctidbybroker = list(dict_dldfilter2acctidbymxz.values())[0]   # 假定只有一个
                    for list_values in list_list_data[start_index_holding + 1:]:
                        if '没有' in list_values[0]:
                            print(f'{acctidbybroker}: {list_values[0]}')
                        else:
                            if len(list_values) == i_list_keys_length:
                                dict_rec_holding = dict(zip(list_keys, list_values))
                                list_ret.append(dict_rec_holding)
                            else:
                                logger_expo.warning(f'{acctidbybroker}_{data_source_type}_{list_values} not added into database')

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
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
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
                fpath = fpath.replace('<YYYY-MM-DD>', self.gl.dt_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath)
                    else:
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
                            if dict_rec_holding['账户'] in dict_dldfilter2acctidbymxz:
                                dict_rec_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_holding['账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['ax_jzpb']:     # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with open(fpath, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['账户编号'] in dict_dldfilter2acctidbymxz:
                                dict_rec_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_holding['账户编号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in [
                'dh_xtqmt', 'zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb', 'swhy_xtpb',
                'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb'
            ]:
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账号'] in dict_dldfilter2acctidbymxz:
                                dict_rec_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_holding['资金账号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['hait_ehfz_api']:   # 有改动
                for acctidbybroker in dict_dldfilter2acctidbymxz:
                    fpath_ = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                    try:
                        with codecs.open(fpath_, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_rec_holding = dict(zip(list_keys, list_values))
                                    dict_rec_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]
                                    list_ret.append(dict_rec_holding)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['huat_matic_tsi']:    # 有改动
                for acctidbybroker in dict_dldfilter2acctidbymxz:
                    fpath_ = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                    try:
                        with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                                continue
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_holding = dict(zip(list_keys, list_values))
                                    # if dict_holding['fund_account'] == acctidbybroker:
                                    dict_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]
                                    list_ret.append(dict_holding)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['gy_htpb', 'gs_htpb', 'gj_htpb']:    # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] in dict_dldfilter2acctidbymxz:
                                dict_rec_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_holding['资金账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['单元序号'] in dict_dldfilter2acctidbymxz:
                                dict_rec_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_holding['单元序号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['yh_apama'] and accttype == 'm':  # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '股份可用', '当前持仓', '持仓成本', '最新价',
                                 '昨日持仓', '冻结数量', '买入冻结', '卖出冻结', '参考盈亏', '参考市值', '是否为担保品',
                                 '担保品折算率', '融资买入股份余额', '融资买入股份可用']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind+1:])   # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            dict_holding['AcctIDByMXZ'] = list(dict_dldfilter2acctidbymxz.values())[0]
                            list_ret.append(dict_holding)
                        else:
                            logger_expo.warning('strange holidng keys of yh_apama %s'%fpath)

            elif data_source_type in ['yh_apama'] and accttype == 'c':  # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '股份可用', '当前持仓', '持仓成本', '最新价',
                                 '昨日持仓', '股东代码', '买入冻结', '买入冻结金额', '卖出冻结', '卖出冻结金额']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind+1:])   # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            dict_holding['AcctIDByMXZ'] = list(dict_dldfilter2acctidbymxz.values())[0]
                            list_ret.append(dict_holding)
                        else:
                            logger_expo.warning('strange holidng keys of yh_apama %s'%fpath)

            elif data_source_type in ['gf_tyt']:
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            if dict_holding['projectid'] in dict_dldfilter2acctidbymxz:
                                dict_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_holding['projectid']]
                                list_ret.append(dict_holding)
            else:
                e = f'Field data_source_type:{data_source_type} in {sheet_name} not exist in basic info!'
                if e not in self.list_warn:
                    self.list_warn.append(e)
                    logger_expo.error(e)

        elif sheet_name == 'short_position':
            if accttype in ['m']:
                if data_source_type in ['zhaos_tdx']:
                    with open(fpath, 'rb') as f:
                        list_datalines = f.readlines()
                        start_index_secloan = None
                        for index, dataline in enumerate(list_datalines):
                            str_dataline = dataline.decode('gbk')
                            if '证券代码' in str_dataline:
                                start_index_secloan = index
                        list_keys = [x.decode('gbk') for x in list_datalines[start_index_secloan].strip().split()]
                        i_list_keys_length = len(list_keys)
                        for dataline in list_datalines[start_index_secloan + 1:]:
                            list_data = dataline.strip().split()
                            if len(list_data) == i_list_keys_length:
                                list_values = [x.decode('gbk') for x in list_data]
                                dict_rec_secloan = dict(zip(list_keys, list_values))
                                secid = dict_rec_secloan['证券代码']
                                if secid[0] in ['0', '1', '3']:
                                    dict_rec_secloan['交易市场'] = '深A'
                                else:
                                    dict_rec_secloan['交易市场'] = '沪A'
                                list_ret.append(dict_rec_secloan)

                elif data_source_type in [
                    'zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb',
                    'swhy_xtpb', 'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb'
                ]:
                    fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                    with codecs.open(fpath, 'rb', 'gbk') as f:
                        list_datalines = f.readlines()
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s'%fpath)
                        else:
                              list_keys = list_datalines[0].strip().split(',')
                        for dataline in list_datalines[1:]:
                            list_values = dataline.strip().split(',')
                            if len(list_values) == len(list_keys):
                                dict_rec_secloan = dict(zip(list_keys, list_values))
                                if dict_rec_secloan['资金账号'] in dict_dldfilter2acctidbymxz:
                                    dict_rec_secloan['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_secloan['资金账号']]
                                    list_ret.append(dict_rec_secloan)

                elif data_source_type in ['hait_ehfz_api']:
                    for acctidbybroker in dict_dldfilter2acctidbymxz:
                        try:
                            fpath_ = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                            with codecs.open(fpath_, 'rb', 'gbk') as f:
                                list_datalines = f.readlines()
                                if len(list_datalines) == 0:
                                    logger_expo.warning('读取空白文件%s'%fpath_)
                                    continue
                                else:
                                      list_keys = list_datalines[0].strip().split(',')
                                for dataline in list_datalines[1:]:
                                    list_values = dataline.strip().split(',')
                                    if len(list_values) == len(list_keys):
                                        dict_rec_secloan = dict(zip(list_keys, list_values))
                                        dict_rec_secloan['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]
                                        list_ret.append(dict_rec_secloan)
                        except FileNotFoundError as e:
                            e = str(e)
                            if e not in self.list_warn:
                                self.list_warn.append(e)
                                logger_expo.error(e)

                elif data_source_type in ['huat_matic_tsi']:
                    for acctidbybroker in dict_dldfilter2acctidbymxz:
                        fpath_ = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                        try:
                            with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                                list_datalines = f.readlines()
                                if len(list_datalines) == 0:
                                    logger_expo.warning('读取空白文件%s'%fpath_)
                                else:
                                      list_keys = list_datalines[0].strip().split(',')
                                for dataline in list_datalines[1:]:
                                    list_values = dataline.strip().split(',')
                                    if len(list_values) == len(list_keys):
                                        dict_secloan = dict(zip(list_keys, list_values))
                                        if dict_secloan['fund_account'] == acctidbybroker:
                                            dict_secloan['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]
                                            list_ret.append(dict_secloan)
                        except FileNotFoundError as e:
                            e = str(e)
                            if e not in self.list_warn:
                                self.list_warn.append(e)
                                logger_expo.error(e)

                elif data_source_type in ['gtja_pluto']:     # 有改动
                    fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                    with codecs.open(fpath, 'rb', 'gbk') as f:
                        list_datalines = f.readlines()
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s'%fpath)
                        else:
                              list_keys = list_datalines[0].strip().split(',')
                        for dataline in list_datalines[1:]:
                            list_values = dataline.strip().split(',')
                            if len(list_values) == len(list_keys):
                                dict_rec_secloan = dict(zip(list_keys, list_values))
                                if dict_rec_secloan['单元序号'] in dict_dldfilter2acctidbymxz:
                                    dict_rec_secloan['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_secloan['单元序号']]
                                    list_ret.append(dict_rec_secloan)
                else:
                    e = f'Field data_source_type:{data_source_type} in {sheet_name} not exist in basic info!'
                    if e not in self.list_warn:
                        self.list_warn.append(e)
                        logger_expo.error(e)

        elif sheet_name == 'order':
            # 先做这几个有secloan的（不然order没意义）:
            if data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb',
                                    'swhy_xtpb', 'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb', 'dh_xtqmt']:
                fpath = fpath.replace('YYYYMMDD', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['资金账号'] in dict_dldfilter2acctidbymxz:
                                dict_rec_order['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_order['资金账号']]
                                list_ret.append(dict_rec_order)
            if data_source_type in ['hait_ehfz_api']:
                for acctidbybroker in dict_dldfilter2acctidbymxz:
                    try:
                        fpath_ = fpath.replace('YYYYMMDD', self.gl.str_today).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                  list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_rec_order = dict(zip(list_keys, list_values))
                                    dict_rec_order['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]
                                    list_ret.append(dict_rec_order)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)
            elif data_source_type in ['huat_matic_tsi']:  # 有改动
                for acctidbybroker in dict_dldfilter2acctidbymxz:
                    try:
                        fpath_ = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                  list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_order = dict(zip(list_keys, list_values))
                                    dict_order['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[acctidbybroker]
                                    list_ret.append(dict_order)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)
            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['单元序号'] in dict_dldfilter2acctidbymxz:
                                dict_rec_order['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_order['单元序号']]
                                list_ret.append(dict_rec_order)
            elif data_source_type in ['yh_apama']:    # 成交明细不是委托明细
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '委托序号', '买卖方向', '股东号', '成交时间',
                                 '成交编号', '成交价格', '成交数量', '成交金额', '成交类型', '委托数量', '委托价格']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        # for other_value in split_line[-1].split('&'):  # order暂无扩展字段
                        #     ind = other_value.find('=')
                        #     list_values.append(other_value[ind + 1:])
                        if len(list_values) == len(list_keys):
                            dict_order = dict(zip(list_keys, list_values))
                            dict_order['AcctIDByMXZ'] = list(dict_dldfilter2acctidbymxz.values())[0]
                            list_ret.append(dict_order)
                        else:
                            logger_expo.warning('strange order keys of yh_apama %s' % fpath)

            elif data_source_type in ['ax_jzpb']:
                fpath = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with open(fpath, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if '信息初始化' in list_values:  # todo 最后一行莫名多出这个（标题和其他行还没有）得改
                            list_values = list_values[:-1]
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['账户编号'] in dict_dldfilter2acctidbymxz:
                                dict_rec_order['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_order['账户编号']]
                                list_ret.append(dict_rec_order)

            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('<YYYY-MM-DD>', self.gl.dt_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['账户'] in dict_dldfilter2acctidbymxz:
                                dict_rec_order['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_order['账户']]
                                list_ret.append(dict_rec_order)

            elif data_source_type in ['gy_htpb', 'gs_htpb', 'gj_htpb']:    # 有改动
                fpath = fpath.replace('<YYYY-MM-DD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] in dict_dldfilter2acctidbymxz:
                                dict_rec_holding['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_rec_holding['资金账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['gf_tyt']:
                fpath = fpath.replace('<YYYY-MM-DD>', self.gl.str_today)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_order = dict(zip(list_keys, list_values))
                            if dict_order['projectid'] in dict_dldfilter2acctidbymxz:
                                dict_order['AcctIDByMXZ'] = dict_dldfilter2acctidbymxz[dict_order['projectid']]
                                list_ret.append(dict_order)
            else:
                e = f'Field data_source_type:{data_source_type} in {sheet_name} not exist in basic info!'
                if e not in self.list_warn:
                    self.list_warn.append(e)
                    logger_expo.error(e)

        else:
            raise ValueError('Wrong sheet name!')
        return list_ret

    def update_rawdata(self):
        dict_raw_data_content_type2col_posttrd_rawdata = {
            'fund': self.gl.col_posttrd_rawdata_fund,
            'holding': self.gl.col_posttrd_rawdata_holding,
            'short_position': self.gl.col_posttrd_rawdata_short_position,
        }

        # 归集fpath
        dict_fpath_posttrd_rawdata2acctinfo = {}
        for dict_acctinfo in self.gl.col_acctinfo.find({'DataDate': self.gl.str_last_trddate, 'DataDownloadMark': 1}):
            fpath_posttrd_rawdata = dict_acctinfo['PostTradeDataFilePath']
            if 'DownloadDataFilter' in dict_acctinfo and dict_acctinfo['DownloadDataFilter']:
                dlddata_filter = dict_acctinfo['DownloadDataFilter']
            else:
                dlddata_filter = dict_acctinfo['AcctIDByBroker']

            if fpath_posttrd_rawdata in dict_fpath_posttrd_rawdata2acctinfo:
                dict_fpath_posttrd_rawdata2acctinfo[fpath_posttrd_rawdata].update(
                    {
                        dlddata_filter: dict_acctinfo['AcctIDByMXZ'],
                        'AcctType': dict_acctinfo['AcctType'],
                        'DataSourceType': dict_acctinfo['DataSourceType']
                    }
                )

            else:
                dict_fpath_posttrd_rawdata2acctinfo[fpath_posttrd_rawdata] = {
                    dlddata_filter: dict_acctinfo['AcctIDByMXZ'],
                    'AcctType': dict_acctinfo['AcctType'],
                    'DataSourceType': dict_acctinfo['DataSourceType']
                }

        # 遍历fpath
        dict_list_upload_recs = {'fund': [], 'holding': [], 'order': [], 'short_position': []}
        for fpath_posttrd_rawdata in dict_fpath_posttrd_rawdata2acctinfo:
            dict_acctinfo = dict_fpath_posttrd_rawdata2acctinfo[fpath_posttrd_rawdata]
            list_fpath_data = fpath_posttrd_rawdata[1:-1].split(',')
            data_source_type = dict_acctinfo["DataSourceType"]
            accttype = dict_acctinfo['AcctType']
            dict_dldfilter2acctidbymxz = dict_acctinfo.copy()  # dlddata_filter: acctidbymxz
            del dict_dldfilter2acctidbymxz['AcctType']
            del dict_dldfilter2acctidbymxz['DataSourceType']

            for i in range(4):  # 清算后批量上传的类型暂定仅有4种
                fpath_post_trddata = list_fpath_data[i]
                if fpath_post_trddata == '':
                    continue
                raw_data_content_type = ['fund', 'holding', 'order', 'short_position'][i]

                if raw_data_content_type in ['order']:
                    continue

                list_dicts_rec = self.read_rawdata_from_trdclient(
                    fpath_post_trddata, raw_data_content_type, data_source_type, accttype, dict_dldfilter2acctidbymxz
                )

                for dict_rec in list_dicts_rec:
                    dict_rec['DataDate'] = self.gl.str_last_trddate
                    dict_rec['AcctType'] = accttype
                    dict_rec['DataSourceType'] = data_source_type

                if list_dicts_rec:
                    dict_list_upload_recs[raw_data_content_type] += list_dicts_rec

        for raw_data_content_type in dict_raw_data_content_type2col_posttrd_rawdata:
            if dict_list_upload_recs[raw_data_content_type]:
                dict_raw_data_content_type2col_posttrd_rawdata[raw_data_content_type].delete_many(
                    {'DataDate': self.gl.str_last_trddate}
                )
                dict_raw_data_content_type2col_posttrd_rawdata[raw_data_content_type].insert_many(
                    dict_list_upload_recs[raw_data_content_type]
                )
        print('Update raw data finished.')

    def formulate_raw_data(self, acctidbymxz, accttype, sheet_type, raw_list):
        list_dicts_fmtted = []
        if accttype in ['c', 'm']:
            # ---------------  FUND 相关列表  ---------------------
            # 净资产 = 总资产-总负债 = NetAsset
            # 现金 = 总资产-总市值 普通户里= available_fund, 在资产负债表里
            # 可用资金 = 可用担保品交易资金， 有很多定义， 不在资产负债表里，交易用
            # 可取资金 = 总资产 - 当日交易股票市值-各种手续费-利息+分红， 不在资产负债表里，交易用
            list_fields_af = ['可用', 'A股可用', '可用数', '现金资产', '可用金额', '资金可用金', '可用余额', 'T+0交易可用金额',
                              'enable_balance', 'fund_asset', '可用资金', 'instravl']
            # 新加：matic_tsi_RZRQ: fund_asset, gtja_pluto:可用资金
            list_fields_ttasset = ['总资产', '资产', '总 资 产', '实时总资产', '单元总资产', '资产总额', '账户总资产',
                                   '担保资产', 'asset_balance', 'assure_asset', '账户资产', '资产总值']
            list_fields_na = ['netasset', 'net_asset', '账户净值', '净资产']   # 尽量避免 '产品净值' 等
            list_fields_kqzj = ['可取资金', '可取金额', 'fetch_balance', '沪深T+1交易可用',  '可取余额', 'T+1交易可用金额',
                                '可取数']   # 'T+1交易可用金额'不算可取
            list_fields_tl = ['总负债', 'total_debit']  #
            # list_fields_cb = []     # 券商没义务提供，得从postdata里找
            list_fields_mktvalue = ['总市值', 'market_value', '证券资产', '证券市值']   # 券商没义务提供，得按long-short算

            # ---------------  Security 相关列表  ---------------------
            list_fields_secid = ['代码', '证券代码', 'stock_code', 'stkcode']
            list_fields_symbol = ['证券名称', 'stock_name', '股票名称', '名称']
            list_fields_shareholder_acctid = ['股东帐户', '股东账号', '股东代码', '股东账户']
            list_fields_exchange = ['市场代码', '交易市场', '交易板块', '板块', '交易所', '交易所名称', '交易市场',
                                    'exchange_type', 'market', '市场类型']

            # 有优先级别的列表
            list_fields_longqty = [
                '当前拥股数量', '股票余额', '拥股数量', '证券余额', '证券数量', '库存数量', '持仓数量', '参考持股', '持股数量', '当前持仓',
                '当前余额', '当前拥股', '实际数量', '实时余额', 'current_amount', 'stkholdqty'
            ]
            dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE',
                                      '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                                      '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                                      '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                                      '上海A股': 'SSE', '深圳A股': 'SZSE',
                                      'SH': 'SSE', 'SZ': 'SZSE',
                                      '上交所A': 'SSE', '深交所A': 'SZSE',
                                      '上证所': 'SSE', '深交所': 'SZSE'}
            dict_ambigu_secidsrc = {'hait_ehfz_api': {'1': 'SZSE', '2': 'SSE'},
                                    'gtja_pluto': {'1': 'SSE', '2': "SZSE"},
                                    'huat_matic_tsi': {'1': 'SSE', '2': 'SZSE'},
                                    'yh_apama': {'0': 'SZSE', '2': 'SSE'},
                                    'ax_jzpb': {'0': 'SZSE', '1': 'SSE'},  # '市场; 市场代码'两个字段
                                    'gf_tyt': {'0': 'SZSE', '1': 'SSE'}}

            # -------------  ORDER 相关列表  ---------------------
            # order委托/entrust除了成交时间等信息最全，不是成交(trade,deal)（没有委托量等）
            #  zxjt_xtpb, zhaos_xtpb只有deal无order； deal/trade？
            # todo 撤单单独列出一个字段 + 买券还券等处理 （huat拆成两个如何合并？）
            #  带数字不明确的得再理一理
            #  OrdID 最好判断下是否有一样的，（数据源可能超级加倍...）
            # 撤单数+成交数=委托数 来判断终态, ordstatus ‘部撤’有时并非终态

            list_fields_cumqty = ['成交数量', 'business_amount', 'matchqty', '成交量']
            list_fields_leavesqty = ['撤单数量', '撤销数量', 'withdraw_amount', 'cancelqty', '撤单量', '已撤数量']
            # apama只有成交，委托待下，成交=终态
            list_fields_side = ['买卖标记', 'entrust_bs',  '委托方向', '@交易类型', 'bsflag', '交易', '买卖标识']
            list_fields_orderqty = ['委托量', 'entrust_amount', '委托数量', 'orderqty']  # XXX_deal 会给不了委托量，委托日期，委托时间，只有成交
            list_fields_ordertime = ['委托时间', 'entrust_time',  'ordertime ', '时间', '成交时间'] # yh
            list_fields_avgpx = ['成交均价', 'business_price', '成交价格', 'orderprice']  # 以后算balance用， exposure不用
            # list_fields_cumamt = ['成交金额', 'business_balance', 'matchamt', '成交额']
            dict_fmtted_side_name = {'买入': 'buy', '卖出': 'sell',
                                     '限价担保品买入': 'buy', '限价买入': 'buy', '担保品买入': 'buy', 'BUY': 'buy', # 担保品=券； 限价去掉,含"...“即可
                                     '限价卖出': 'sell', '限价担保品卖出': 'sell', '担保品卖出': 'sell', 'SELL': 'sell',
                                     '0B': 'buy', '0S': 'sell', '证券买入': 'buy', '证券卖出': 'sell',
                                     '限价融券卖出': 'sell short', '融券卖出': 'sell short',  # 快速交易的 hait=11
                                     '现券还券划拨': 'XQHQ',  '现券还券划拨卖出': 'XQHQ',# 快速交易的 hait=15, gtja=34??
                                     '买券还券划拨': 'MQHQ', '买券还券': 'MQHQ', '限价买券还券': 'MQHQ',  # 快速交易的 hait=13
                                     '撤单': 'cancel', 'ZR': 'Irrelevant', 'ZC': 'Irrelevant'}  # entrust_bs表方向时值为1，2
            dict_ambigu_side_name = {'hait_ehfz_api': {'1': 'buy', '2': 'sell', '12': 'sell short',
                                                   '15': 'XQHQ', '13': 'MQHQ', '0': 'cancel'},
                                     'gtja_pluto': {'1': 'buy', '2': 'sell', '34': 'MQHQ', '32': 'sell short',
                                                    '31': 'buy', '33': 'sell', '36': 'XQHQ'},  # 融资买入， 卖券还款
                                     'huat_matic_tsi': {'1': 'buy', '2': 'sell'}}  # 信用户在后面讨论（需要两个字段拼起来才行）
            # dict_datasource_ordstatus = {
            #     # 参考FIX：New已报； Partially Filled=部成待撤/部成，待撤=PendingCancel不算有效cumqty,中间态
            #     # 国内一般全成，部撤等都表示最终态，cumqty的数值都是有效的(Filled, Partially Canceled)，其他情况的cumqty不能算
            #     # 部撤 Partially Canceled(自己命名的）
            #     'hait_ehfz_api': {'5': 'Partially Canceled', '8': 'Filled', '6': 'Canceled'},
            #     'gtja_pluto': {'4': 'New', '6': 'Partially Filled', '7': 'Filled', '8': 'Partially Canceled',
            #                    '9': 'Canceled', '5': 'Rejected', '10': 'Pending Cancel', '2': 'Pending New'},
            #     'yh_apama': {'2': 'New', '5': 'Partially Filled', '8': 'Filled', '7': 'Partially Filled',  # todo 看表确认
            #                  '6': 'Canceled', '9': 'Rejected', '3': 'Pending Cancel', '1': 'Pending New'},
            #     'huat_matic_tsi': {'2': 'New', '7': 'Partially Filled', '8': 'Filled', '5': 'Partially Filled',
            #                        '6': 'Canceled', '9': 'Rejected', '4': 'Pending Cancel', '1': 'Pending New'},
            #     'zx_wealthcats': {'部撤': 'Partially Filled', '全成': 'Filled', '全撤': 'Canceled', '废单': 'Rejected'},
            #     'xtpb': {'部成': 'Partially Filled', '已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected', '部撤': 'Partially Filled'},
            #     'gt_tyt': {'8': 'Filled'},
            #     'ax_jzpb': {'已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected',
            #                 '部撤': 'Partially Filled', '已报': 'New'},
            #     'htpb': {'已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected', '部撤': 'Partially Filled'},
            #     }
            list_date_format = ['%Y%m%d']
            list_time_format = ['%H%M%S', '%H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S']
            # -------------  SECURITY LOAN 相关列表  ---------------------
            # todo 加hait_xtpb; huat_matic参考其手册;
            #  pluto 合约类型，合约状态里的1和huat里的1指代一个吗？
            #  这块 有不少问题！！！目前只关注short暂不会出错
            list_fields_shortqty = ['未还合约数量', 'real_compact_amount', '未还负债数量', '发生数量']  # 未还合约数量一般是开仓数量
            # 合约和委托没有关系了，但是用contract还是compact(券商版）?
            list_fields_contractqty = ['合约开仓数量', 'business_amount', '成交数量']  # 国外sell short约为“融券卖出”
            # list_fields_contracttype = ['合约类型', 'compact_type']  # 一定能分开 锁券与否
            # list_fields_contractstatus = ['合约状态', 'compact_status', '@负债现状']  # filled='完成'那不是委托？融资融券能用
            list_fields_opdate = ['合约开仓日期', 'open_date', '发生日期']  # FIX 合约: contract
            list_fields_sernum = ['成交编号', '合同编号', 'entrust_no', '委托序号', '合约编号', '合同号', 'instr_no', '成交序号',
                                  '订单号', '委托编号']
            # SerialNumber 券商不统一，目前方便区分是否传了两遍..然而entrust_no还是重复 (RZRQ里的business_no)可以
            list_fields_compositesrc = []  # todo CompositeSource

            # todo: 其它名字’开仓未归还‘，私用融券（专项券池）等得之后补上, 像上面做一个 ambigu区分
            #  遇到bug，pluto vs matic 2指代不一样的
            # Note3. contractstatus, contracttype 有些标准乱，以后有用处理
            # dict_contractstatus_fmt = {'部分归还': '部分归还', '未形成负债': None, '已归还': '已归还',
            #                            '0': '开仓未归还', '1': '部分归还', '5': None,
            #                            '2': '已归还/合约过期', '3': None,
            #                            '未归还': '开仓未归还', '自行了结': None}  # 有bug了...pluto vs matic
            #
            # dict_contracttype_fmt = {'融券': 'rq', '融资': 'rz',
            #                          '1': 'rq', '0': 'rz',
            #                          '2': '其它负债/？？？'}  # 一般没有融资, 其它负债（2）

            if sheet_type == 'fund':  # cash
                list_dicts_fund = raw_list
                if list_dicts_fund is None:
                    list_dicts_fund = []
                for dict_fund in list_dicts_fund:
                    data_source = dict_fund['DataSourceType']
                    cash = None
                    avlfund = None  # 'AvailableFund'
                    ttasset = None  # 'TotalAsset'
                    mktvalue = None
                    netasset = None
                    kqzj = None     # 可取资金
                    total_liability = None

                    # 分两种情况： 1. cash acct: 至少要有cash 2. margin acct: 至少要有ttasset

                    flag_check_new_name = True  # 用来弥补之前几个list的缺漏
                    for field_af in list_fields_af:
                        if field_af in dict_fund:
                            avlfund = float(dict_fund[field_af])
                            flag_check_new_name = False
                    err = 'unknown available_fund name %s' % data_source

                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_fund)
                            logger_expo.debug((err, dict_fund))

                    if accttype == 'm':
                        flag_check_new_name = True
                        for field_ttasset in list_fields_ttasset:
                            if field_ttasset in dict_fund:
                                ttasset = float(dict_fund[field_ttasset])
                                flag_check_new_name = False
                        err = 'unknown total asset name %s' % data_source

                        if flag_check_new_name:
                            if data_source not in ['gy_htpb', 'gs_htpb']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                            else:
                                ttasset = float(dict_fund['产品总资产'])

                        flag_check_new_name = True
                        for field_mktv in list_fields_mktvalue:
                            if field_mktv in dict_fund:
                                mktvalue = float(dict_fund[field_mktv])
                                flag_check_new_name = False
                        err = 'unknown total market value name %s'%data_source

                        if flag_check_new_name:
                            if data_source not in ['gtja_pluto']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                        else:
                            cash = ttasset - mktvalue

                        # 读取净资产，总负债，或者两者之中推出另一个
                        for field_na in list_fields_na:
                            if field_na in dict_fund:
                                netasset = float(dict_fund[field_na])

                        for field_tl in list_fields_tl:
                            if field_tl in dict_fund:
                                total_liability = float(dict_fund[field_tl])

                        if total_liability and netasset:
                            delta = total_liability + netasset - ttasset
                            if abs(delta) > 1:
                                err = '券商%s数据错误：总资产 - 总负债 - 净资产 =%d' % (data_source, -delta)
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    logger_expo.error((err, dict_fund))
                                    print(err, dict_fund)
                                # 默认总资产正确：
                                netasset = ttasset - total_liability
                        else:
                            if data_source in ['gy_htpb', 'gs_htpb', 'gj_htpb']:
                                netasset = float(dict_fund['产品净值'])
                            elif data_source in []:  # 没有净资产等字段
                                pass
                            elif not(total_liability is None):
                                netasset = ttasset - total_liability
                            elif not(netasset is None):
                                total_liability = ttasset - netasset
                            else:
                                err = 'unknown net asset or liability name %s'%data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))

                    else:
                        flag_check_new_name = True
                        for field_ttasset in list_fields_ttasset + list_fields_na:
                            if field_ttasset in dict_fund:
                                ttasset = float(dict_fund[field_ttasset])
                                flag_check_new_name = False
                        err = 'unknown total asset name %s'%data_source
                        if flag_check_new_name:
                            if data_source not in ['gy_htpb', 'gs_htpb', 'gj_htpb']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                            else:
                                ttasset = float(dict_fund['产品总资产'])
                        netasset = ttasset
                        total_liability = 0
                        cash = avlfund

                    flag_check_new_name = True
                    for field_kqzj in list_fields_kqzj:
                        if field_kqzj in dict_fund:
                            kqzj = float(dict_fund[field_kqzj])
                            flag_check_new_name = False
                    err = 'unknown 可取资金 name %s'%data_source
                    if flag_check_new_name and data_source not in ['gf_tyt', 'zhaos_xtpb']:   # 他们没有可取
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_fund)
                            logger_expo.debug((err, dict_fund))
                        # flt_cash = flt_ttasset - stock_longamt - etf_longamt - ce_longamt

                    dict_fund_fmtted = {
                        'DataDate': self.gl.str_last_trddate,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'Cash': cash,
                        'NetAsset': netasset,
                        'AvailableFund': avlfund,  # flt_approximate_na?
                        'TotalAsset': ttasset,
                        'TotalLiability': total_liability,
                        'KQZJ': kqzj  # 总股本*每股价值 = 证券市值, 之后补上
                    }
                    list_dicts_fmtted.append(dict_fund_fmtted)

            elif sheet_type == 'holding':
                # 2.整理holding
                # 2.1 rawdata(无融券合约账户)
                list_dicts_holding = raw_list

                for dict_holding in list_dicts_holding:  # 不必 list_dicts_holding.keys()
                    secid = None
                    secidsrc = None
                    symbol = None
                    data_source = dict_holding['DataSourceType']
                    longqty = 0
                    # shortqty = 0
                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_holding:
                            secid = str(dict_holding[field_secid])
                            flag_check_new_name = False
                    err = 'unknown secid name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_holding)
                            logger_expo.debug((err, dict_holding))

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_holding:
                            shareholder_acctid = str(dict_holding[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_holding:
                            try:
                                if data_source in dict_ambigu_secidsrc:
                                    digit_exchange = str(dict_holding[field_exchange])
                                    secidsrc = dict_ambigu_secidsrc[data_source][digit_exchange]
                                else:
                                    exchange = dict_holding[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_holding)
                                    logger_expo.debug((err, dict_holding))
                            flag_check_new_name = False
                            break

                    err = 'unknown security source name %s' % data_source
                    if flag_check_new_name:
                        if secid[0] in ['6']:
                            secidsrc = 'SSE'
                        else:
                            secidsrc = 'SZSE'

                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err)
                            logger_expo.warning(err)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_holding:
                            symbol = str(dict_holding[field_symbol])
                            flag_check_new_name = False
                    err = 'unknown symbol name %s' % data_source
                    if flag_check_new_name:
                        if data_source in ['hait_ehfz_api', 'yh_apama', 'gf_tyt']:
                            symbol = '???'  # 不管，需要可以用wind获取
                        else:
                            if err not in self.list_warn:
                                self.list_warn.append(err)
                                print(err, dict_holding)
                                logger_expo.debug((err, dict_holding))

                    flag_check_new_name = True
                    for field_longqty in list_fields_longqty:
                        if field_longqty in dict_holding:
                            longqty = float(dict_holding[field_longqty])
                            flag_check_new_name = False
                    err = 'unknown longqty name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_holding)
                            logger_expo.debug((err, dict_holding))

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = self.gl.get_mingshi_sectype_from_code(windcode)
                    if sectype != 'IrrelevantItem':
                        if windcode in self.gl.dict_fmtted_wssdata_last_trddate['Close']:
                            close = self.gl.dict_fmtted_wssdata_last_trddate['Close'][windcode]
                        else:
                            close = 0
                    else:
                        close = 0
                    longamt = longqty * close

                    dict_holding_fmtted = {
                        'DataDate': self.gl.str_last_trddate,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'SecurityID': secid,
                        'Close': close,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'LongAmt': longamt,
                    }
                    list_dicts_fmtted.append(dict_holding_fmtted)

            elif sheet_type == 'short_position':
                list_dicts_secloan = raw_list
                for dict_secloan in list_dicts_secloan:
                    secid = None
                    secidsrc = None
                    symbol = None
                    # longqty = 0
                    shortqty = 0
                    contractstatus = None
                    contracttype = None
                    contractqty = None
                    opdate = None
                    sernum = None
                    compositesrc = None
                    data_source = dict_secloan['DataSourceType']

                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_secloan:
                            secid = str(dict_secloan[field_secid])
                            flag_check_new_name = False
                    err = 'unknown field_secid name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_secloan:
                            shareholder_acctid = str(dict_secloan[field_shareholder_acctid])
                            if len(shareholder_acctid) == 0:
                                continue
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_secloan:
                            try:
                                if data_source in dict_ambigu_secidsrc:
                                    str_digit_exchange = str(dict_secloan[field_exchange])
                                    secidsrc = dict_ambigu_secidsrc[data_source][str_digit_exchange]
                                else:
                                    exchange = dict_secloan[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_secloan)
                                    logger_expo.debug(err, dict_secloan)
                            flag_check_new_name = False
                    err = 'unknown security source name %s' % data_source

                    if flag_check_new_name:
                        if secid[0] in ['6', '5']:
                            secidsrc = 'SSE'
                        elif secid[0] in ['3', '0']:
                            secidsrc = 'SZSE'
                        else:
                            secidsrc = input(f'请输入代码{secid}的交易所: SSE/SZSE')
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err)
                            logger_expo.warning(err, dict_secloan)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_secloan:
                            symbol = str(dict_secloan[field_symbol])
                            flag_check_new_name = False
                    err = 'unknown field symbol name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_shortqty in list_fields_shortqty:
                        if field_shortqty in dict_secloan:
                            shortqty = float(dict_secloan[field_shortqty])
                            flag_check_new_name = False
                    err = 'unknown field shortqty name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_contractqty in list_fields_contractqty:
                        if field_contractqty in dict_secloan:
                            contractqty = str(dict_secloan[field_contractqty])
                        flag_check_new_name = False
                    err = 'unknown field contractqty name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_sernum in list_fields_sernum:
                        if field_sernum in dict_secloan:
                            sernum = str(dict_secloan[field_sernum])
                            flag_check_new_name = False
                    err = 'unknown field serum name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_opdate in list_fields_opdate:
                        if field_opdate in dict_secloan:
                            opdate = str(dict_secloan[field_opdate])
                            flag_check_new_name = False
                            datetime_obj = None
                            # 和order共用 date格式
                            for date_format in list_date_format:
                                try:
                                    datetime_obj = datetime.strptime(opdate, date_format)
                                except ValueError:
                                    pass
                            if datetime_obj:
                                opdate = datetime_obj.strftime('%Y%m%d')
                            else:
                                err = 'Unrecognized trade date format %s' % data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_secloan)
                                    logger_expo.debug((err, dict_secloan))

                    if flag_check_new_name:
                        err = 'unknown field opdate name %s' % data_source
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_compositesrc in list_fields_compositesrc:
                        if field_compositesrc in dict_secloan:
                            compositesrc = str(dict_secloan[field_compositesrc])
                            flag_check_new_name = False
                    if flag_check_new_name and list_fields_compositesrc:
                        err = 'unknown field_compositesrc name %s' % data_source
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = self.gl.get_mingshi_sectype_from_code(windcode)
                    if sectype != 'IrrelevantItem':
                        close = self.gl.dict_fmtted_wssdata_last_trddate['Close'][windcode]
                    else:
                        close = 0
                    shortamt = shortqty * close

                    dict_secloan_fmtted = {
                        'DataDate': self.gl.str_last_trddate,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'SerialNumber': sernum,
                        'OpenPositionDate': opdate,
                        'ContractStatus': contractstatus,
                        'ContractType': contracttype,
                        'ContractQty': contractqty,
                        'CompositeSource': compositesrc,
                        'ShortQty': shortqty,
                        'ShortAmt': shortamt
                    }
                    list_dicts_fmtted.append(dict_secloan_fmtted)

            else:
                raise ValueError('Unknown f_h_o_s_mark')

        elif accttype in ['f']:
            list_dicts_future_fund = raw_list
            for dict_fund_future in list_dicts_future_fund:
                avlfund = dict_fund_future['DYNAMICBALANCE']
                acctidbymxz = dict_fund_future['AcctIDByMXZ']
                kqzj = dict_fund_future['USABLECURRENT']
                dict_future_fund_fmtted = {
                    'DataDate': self.gl.str_last_trddate,
                    'AcctIDByMXZ': acctidbymxz,
                    'DataSourceType': 'trader_api',
                    'Cash': avlfund,   # 期货户里不能拿券当担保品，全是现金
                    'NetAsset': avlfund,
                    'AvailableFund': avlfund,
                    'TotalAsset': None,  # 总资产大致是LongAmt
                    'TotalLiability': None,
                    'KQZJ': kqzj  # 总股本*每股价值 = 证券市值, 之后补上
                }
                list_dicts_fmtted.append(dict_future_fund_fmtted)
            # 期货holding直接放到 position里
        else:
            logger_expo.debug('Unknown account type in basic account info.')
        return list_dicts_fmtted

    def update_fmtdata(self):
        dict_rdct2col_posttrd_rawdata = {
            'fund': self.gl.col_posttrd_rawdata_fund,
            'holding': self.gl.col_posttrd_rawdata_holding,
            'short_position': self.gl.col_posttrd_rawdata_short_position
        }

        dict_rdct2col_posttrd_fmtdata = {
            'fund': self.gl.col_posttrd_fmtdata_fund,
            'holding': self.gl.col_posttrd_fmtdata_holding,
            'short_position': self.gl.col_posttrd_fmtdata_short_position
        }

        # 两层： {rdct:{acctidbymxz: list_dicts}}
        dict_rdct2list_dicts_posttrd_fmtdata = {'fund': [], 'holding': [], 'short_position': []}

        dict_rdct2dict_acctidbymxz2list_dicts_posttrd_rawdata = {'fund': {}, 'holding': {}, 'short_position': {}}
        for rdct, col_posttrd_rawdata in dict_rdct2col_posttrd_rawdata.items():
            for dict_posttrd_rawdata in col_posttrd_rawdata.find({'DataDate': self.gl.str_last_trddate}):
                acctidbymxz = dict_posttrd_rawdata['AcctIDByMXZ']
                dict_posttrd_rawdata['DataSourceType'] = self.dict_acctidbymxz2acctinfo['DataSourceType'][acctidbymxz]
                if acctidbymxz in dict_rdct2dict_acctidbymxz2list_dicts_posttrd_rawdata[rdct]:
                    dict_rdct2dict_acctidbymxz2list_dicts_posttrd_rawdata[rdct][acctidbymxz].append(dict_posttrd_rawdata)
                else:
                    dict_rdct2dict_acctidbymxz2list_dicts_posttrd_rawdata[rdct][acctidbymxz] = [dict_posttrd_rawdata]

        for dict_acctinfo in self.gl.col_acctinfo.find({'DataDate': self.gl.str_last_trddate, 'DataDownloadMark': 1}):
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']

            for rdct in dict_rdct2dict_acctidbymxz2list_dicts_posttrd_rawdata.keys():
                if acctidbymxz in dict_rdct2dict_acctidbymxz2list_dicts_posttrd_rawdata[rdct]:
                    list_dicts_posttrd_rawdata = dict_rdct2dict_acctidbymxz2list_dicts_posttrd_rawdata[rdct][acctidbymxz]
                    list_dicts_posttrd_fmtdata = self.formulate_raw_data(
                        acctidbymxz, accttype, rdct, list_dicts_posttrd_rawdata
                    )
                    dict_rdct2list_dicts_posttrd_fmtdata[rdct] += list_dicts_posttrd_fmtdata
                else:
                    continue

        for rdct in dict_rdct2list_dicts_posttrd_fmtdata.keys():
            dict_rdct2col_posttrd_fmtdata[rdct].delete_many({'DataDate': self.gl.str_last_trddate})
            if dict_rdct2list_dicts_posttrd_fmtdata[rdct]:
                dict_rdct2col_posttrd_fmtdata[rdct].insert_many(dict_rdct2list_dicts_posttrd_fmtdata[rdct])

        print('Update fmtdata finished.')

    def update_col_posttrd_position(self):
        list_dicts_posttrd_position = []
        dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data = {}

        for col_name in ['post_trade_formatted_data_holding', 'post_trade_formatted_data_short_position']:
            for dict_posttrd_fmtdata in self.gl.db_posttrddata[col_name].find(
                    {'DataDate': self.gl.str_last_trddate}, {'_id': 0}
            ):
                tuple_acctidbymxz_secid_secidsrc = (
                    dict_posttrd_fmtdata['AcctIDByMXZ'],
                    dict_posttrd_fmtdata['SecurityID'],
                    dict_posttrd_fmtdata['SecurityIDSource'],
                )

                if tuple_acctidbymxz_secid_secidsrc in dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data:
                    if col_name in dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data[tuple_acctidbymxz_secid_secidsrc]:
                        dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data[tuple_acctidbymxz_secid_secidsrc][col_name].append(dict_posttrd_fmtdata)
                    else:
                        dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data[tuple_acctidbymxz_secid_secidsrc].update({col_name: [dict_posttrd_fmtdata]})
                else:
                    if col_name == 'post_trade_formatted_data_holding':
                        dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data.update(
                            {
                                tuple_acctidbymxz_secid_secidsrc: {
                                    'post_trade_formatted_data_holding': [dict_posttrd_fmtdata],
                                    'post_trade_formatted_data_short_position': [],
                                }
                            }
                        )
                    elif col_name == 'post_trade_formatted_data_holding':
                        dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data.update(
                            {
                                tuple_acctidbymxz_secid_secidsrc: {
                                    'post_trade_formatted_data_holding': [],
                                    'post_trade_formatted_data_short_position': [dict_posttrd_fmtdata],
                                }
                            }
                        )
                    else:
                        raise ValueError('Unknown col_name.')

        for tuple_acctidbymxz_secid_secidsrc in dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data:
            acctidbymxz = tuple_acctidbymxz_secid_secidsrc[0]
            secid = tuple_acctidbymxz_secid_secidsrc[1]
            secidsrc = tuple_acctidbymxz_secid_secidsrc[2]

            list_dicts_posttrd_holding = dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data[tuple_acctidbymxz_secid_secidsrc]['post_trade_formatted_data_holding']
            list_dicts_posttrd_short_position = dict_tuple_acctidbymxz_secid_secidsrc2dict_col_name2data[tuple_acctidbymxz_secid_secidsrc]['post_trade_formatted_data_short_position']

            longqty = 0
            shortqty = 0
            longamt = 0
            shortamt = 0

            for dict_posttrd_holding in list_dicts_posttrd_holding:
                longqty += dict_posttrd_holding['LongQty']
                longamt += dict_posttrd_holding['LongAmt']

            for dict_posttrd_short_position in list_dicts_posttrd_short_position:
                shortqty += dict_posttrd_short_position['ShortQty']
                shortamt += dict_posttrd_short_position['ShortAmt']

            netamt = longamt - shortamt

            if longqty or shortqty:
                dict_posttrd_position = {
                    'DataDate': self.gl.str_last_trddate,
                    'AcctIDByMXZ': acctidbymxz,
                    'SecurityID': secid,
                    'SecurityIDSource': secidsrc,
                    'LongQty': longqty,
                    'ShortQty': shortqty,
                    'NetQty': longqty - shortqty,
                    'LongAmt': longamt,
                    'ShortAmt': shortamt,
                    'NetAmt': netamt,
                }
                list_dicts_posttrd_position.append(dict_posttrd_position)

        if list_dicts_posttrd_position:
            self.gl.col_posttrd_position.delete_many({'DataDate': self.gl.str_last_trddate})
            self.gl.col_posttrd_position.insert_many(list_dicts_posttrd_position)

    def run(self):
        self.update_rawdata()
        self.update_fmtdata()
        self.update_col_posttrd_position()
        print(f'Upload post trade data finished at {datetime.now().strftime("%H:%M:%S")}')


if __name__ == '__main__':
    task = UpdatePostTradeData()
    task.run()
