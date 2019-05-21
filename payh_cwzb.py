from pprint import pprint
from pandas import DataFrame
from dateutil.relativedelta import relativedelta
from payh_sbxx import handle
import pandas as pd
import numpy as np
np.seterr(divide='ignore', invalid='ignore')
import os
import sys
sys.path.append(os.path.abspath('Tools'))
import common
import datetime


class handle_cwzb():
    def __init__(self,nsrsbh):
        self.db_handle = common.oracle_handle_payh()
        self.nsrsbh=nsrsbh
        self_cw_data = None

    def get_cw_data(self):
        sql = '''select a.*,b.* from zcfzb_xm a, lrb_xm b
            where a.nsrsbh=b.nsrsbh and a.nsrsbh='{0}'
            '''.format(self.nsrsbh)
        result, col = self.db_handle.query(sql)
        self.cw_data = common.dataframe(result, col)
        pprint(self.cw_data)

    def get_cwzb_info(self):
        data = self.cw_data
        cwzb = DataFrame(index=[0])
        cwzb.loc[:,'zsbzsbl'] = np.divide(data['yuszk'], data['yysr'])
        cwzb.loc[:,'xjldfzl'] = np.divide(data['hbzj'], data['ldfzhj'])
        cwzb.loc[:,'ldbl'] = np.divide(data['ldzchj'], data['ldfzhj'])
        cwzb.loc[:,'sdbl'] = np.divide(data['ldzchj'] - data['ch'],
                                      data['ldfzhj'])
        cwzb.loc[:,'sdbl2'] = np.divide(
            data['hbzj'] + data['dqtz'] + data['yszk'], data['ldfzhj'])
        cwzb.loc[:,'zcfzl'] = np.divide(data['fzhj'], data['zchj'])
        cwzb.loc[:,'zqfzl'] = np.divide(data['fzhj'] - data['ldfzhj'],
                                       data['zchj'])
        cwzb.loc[:,'zqzcshl'] = np.divide(
            data['syzqy'] + data['fzhj'] - data['ldfzhj'],
            data['gdzchj'] + data['cqgqtz'])
        cwzb.loc[:,'jqzcshl_1'] = np.divide(
            data['syzqy'] + data['fzhj'] + data['ldfzhj'],
            data['gdzchj'] + data['cqgqtz'] - data['zj'])
        cwzb.loc[:,'cqbl'] = np.divide(data['fzhj'], data['syzqy'])
        cwzb.loc[:,'zxjzzwl'] = np.divide(data['fzhj'],
                                         data['syzqy'] - data['wxzc'])
        cwzb.loc[:,'shlxbs'] = np.divide(data['lrze'] + data['cwfy'],
                                        data['cwfy'])
        cwzb.loc[:,'zhlxbs_1'] = np.divide(
            data['lrze'] + data['cwfy'] + data['zj'] - data['zj_1'],
            data['cwfy'])
        cwzb.loc[:,'zqfzzzzzjbl'] = np.divide(data['fzhj'] - data['ldfzhj'],
                                             data['ldzchj'] - data['ldfzhj'])
        cwzb.loc[:,'zzzjzwb'] = np.divide(data['ldzchj'] - data['ldfzhj'],
                                         data['fzhj'])
        cwzb.loc[:,'zxzczwl'] = np.divide(data['fzhj'],
                                         data['ldzchj'] + data['gdzchj'])
        cwzb.loc[:,'zhfzzcb'] = np.divide(
            data['dqjk'] + data['cqjk'] + data['ynndqdcqfz'], data['zchj'])
        cwzb.loc[:,'zhzwfgb'] = np.divide(
            data['lrze'] + data['cwfy'] + data['zj'] - data['zj_1'],
            data['dqjk'] + data['cqjk'] + data['ynndqdcqfz'])
        cwzb.loc[:,'zhzwzbjzb'] = np.divide(
            data['dqjk'] + data['cqjk'] + data['ynndqdcqfz'], data['syzqy'])
        cwzb.loc[:,'zewbebitda'] = np.divide(
            data['fzhj'],
            data['lrze'] + data['cwfy'] + data['zj'] - data['zj_1'])
        cwzb.loc[:,'chldfzb'] = np.divide(data['ch'], data['ldfzhj'])
        cwzb.loc[:,'xjbl'] = np.divide(
            data['hbzj'] + data['dqtz'] + data['ysgl'] + data['yslx'] +
            data['yspj'], data['ldfzhj'])
        cwzb.loc[:,'jksrb'] = np.divide(
            data['yfpj'] + data['cqjk'] + data['dqjk'] + data['ynndqdcqfz'],
            data['yysr'])
        cwzb.loc[:,'jksrb_1'] = np.divide(data['cqjk'] + data['dqjk'],
                                         data['yysr'])
        cwzb.loc[:,'jksrb_2'] = np.divide(
            data['cqjk'] + data['dqjk'] + data['ynndqdcqfz'], data['yysr'])
        cwzb.loc[:,'zqjksrb'] = np.divide(data['cqjk'], data['yysr'])
        cwzb.loc[:,'dqjksrb'] = np.divide(
            data['yfpj'] + data['dqjk'] + data['ynndqdcqfz'], data['yysr'])
        cwzb.loc[:,'qtzfksrb'] = np.divide(data['qtyfk'], data['yysr'])
        cwzb.loc[:,'zfgzsrb'] = np.divide(data['qfzgxc'], data['yysr'])
        cwzb.loc[:,'zfsjsrb'] = np.divide(data['yjsf'], data['yysr'])
        cwzb.loc[:,'zqzbfzl'] = np.divide(data['fzhj'] - data['ldfzhj'],
                                         data['zchj'] - data['ldfzhj'])
        cwzb.loc[:,'lxbzbs'] = np.divide(
            data['sdsfy'] + data['jlr'] + data['cwfy'], data['cwfy'])
        cwzb.loc[:,'ebitbdqjk'] = np.divide(
            data['sdsfy'] + data['jlr'] + data['cwfy'], data['dqjk'])
        cwzb.loc[:,'dqjkbebit'] = np.divide(
            data['dqjk'], data['sdsfy'] + data['jlr'] + data['cwfy'])
        cwzb.loc[:,'zzlrzjkb'] = np.divide(data['cqjk'] + data['dqjk'],
                                          data['yylr'])
        cwzb.loc[:,'zzlrzjkb_1'] = np.divide(
            data['yfpj'] + data['cqjk'] + data['dqjk'] + data['ynndqdcqfz'],
            data['yylr'])
        cwzb.loc[:,'zzlrzjkb_2'] = np.divide(
            data['yfpj'] + data['dqjk'] + data['cqjk'] + data['ynndqdcqfz'],
            data['yylr'])
        cwzb.loc[:,'kczszkhdfzbl'] = np.divide(data['hbzj'], data['zchj'])
        cwzb.loc[:,'xjb'] = np.divide(data['hbzj'], data['zchj'])
        cwzb.loc[:,'qzfzbl'] = np.divide(data['syzqy'], data['fzhj'])
        cwzb.loc[:,'ldzczb'] = np.divide(data['ldzchj'], data['zchj'])
        cwzb.loc[:,'ldfzzb'] = np.divide(data['ldfzhj'], data['zchj'])
        cwzb.loc[:,'gdzcqzbl'] = np.divide(data['gdzchj'], data['syzqy'])
        cwzb.loc[:, 'gdzcjqtzczb'] = np.divide(data['gdzchj'] + data['wxzc'],
                                               data['zchj'])
        cwzb.loc[:,'gdzccxl'] = np.divide(
            data['gdzcyj'] - data['zj'] + data['gdzcyj_1'] - data['zj_1'],
            data['gdzcyj'] + data['gdzcyj_1'])
        cwzb.loc[:,'ldfzl'] = np.divide(data['ldfzhj'], data['fzhj'])
        cwzb.loc[:,'zzzbzb'] = np.divide(data['ldzchj'] - data['ldfzhj'],
                                        data['zchj'])
        cwzb.loc[:,'zhfzjg'] = np.divide(
            data['dqjk'] + data['ynndqdcqfz'],
            data['dqjk'] + data['cqjk'] + data['ynndqdcqfz'])
        cwzb.loc[:,'szzqzbl'] = np.divide(data['syzqy'], data['zchj'])
        cwzb.loc[:,'fldfzqzb'] = np.divide(data['zchj'] - data['ldzchj'],
                                          data['syzqy'])
        cwzb.loc[:,'zqfzzcb'] = np.divide(data['fzhj'] - data['ldfzhj'],
                                         data['zchj'])
        cwzb.loc[:,'zzczzl'] = np.divide(data['yysr'],
                                        (data['zchj_1'] + data['zchj']) * 0.5)
        cwzb.loc[:,'zszkzzl'] = np.divide(data['yysr'],
                                         (data['yszk_1'] + data['yszk']) * 0.5)
        cwzb.loc[:, 'zszkzzl_1'] = np.divide(
            data['yysr'], (data['yszk_1'] + data['yszk']) * 0.5)
        cwzb.loc[:,'ldzczzl'] = np.divide(
            data['yysr'], (data['ldzchj_1'] + data['ldzchj']) * 0.5)
        cwzb.loc[:,'szzqzzzl'] = np.divide(
            data['yysr'], (data['syzqy_1'] + data['syzqy']) * 0.5)
        cwzb.loc[:,'gdzczzl'] = np.divide(
            data['yysr'],
            (data['gdzcyj_1'] - data['zj_1'] + data['gdzcyj'] - data['zj']) *
            0.5)
        cwzb.loc[:,'chzzl'] = np.divide(data['yycb'],
                                       (data['ch'] + data['ch_1']) * 0.5)
        cwzb.loc[:,'zzczzts'] = np.divide((data['zchj_1'] + data['zchj']) * 0.5,
                                         data['yysr'])
        cwzb.loc[:,'zszkzzts'] = np.divide(
            (data['yszk_1'] + data['yszk']) * 0.5, data['yysr'])
        cwzb.loc[:,'zszkzzts_1'] = np.divide(
            (data['yszk_1'] + data['yszk']) * 0.5, data['yysr'])
        cwzb.loc[:,'ldzczzts'] = np.divide(
            (data['ldzchj_1'] + data['ldzchj']) * 0.5, data['yysr'])
        cwzb.loc[:,'szzqzzzts'] = np.divide(
            (data['syzqy_1'] + data['syzqy']) * 0.5, data['yysr'])
        cwzb.loc[:,'gdzczzts'] = np.divide(
            (data['gdzcyj_1'] + data['gdzcyj'] - data['zj_1'] - data['zj']) *
            0.5, data['yysr'])
        cwzb.loc[:,'chzzts'] = np.divide(data['ch_1'] + data['ch'],
                                        data['yysr'])

        cwzb.loc[:,'zclrl'] = np.divide(data['lrze'],
                                       (data['zchj_1'] + data['zchj']) * 0.5)
        cwzb.loc[:,'zzcszl'] = np.divide(data['jlr'], data['zchj'])
        cwzb.loc[:,'zzcjll'] = np.divide(data['jlr'], data['zchj'])
        cwzb.loc[:,'zzcbcl'] = np.divide(data['lrze'] + data['cwfy'],
                                        (data['zchj_1'] + data['zchj'] * 0.5))
        cwzb.loc[:, 'zzczzl_1'] = np.divide(data['zchj_1'] + data['zchj'],
                                            data['yysr'])
        cwzb.loc[:,'zszkzzl_2'] = np.divide(data['yszk_1'] + data['yszk'],
                                           data['yysr'])
        cwzb.loc[:,'zszkzzl_3'] = np.divide(data['yszk_1'] + data['yszk'],
                                           data['yysr'])
        cwzb.loc[:,'ldzczzl_1'] = np.divide(data['ldzchj_1'] + data['ldzchj'],
                                           data['yysr'])
        cwzb.loc[:,'szzqzzzl_1'] = np.divide(data['syzqy_1'] + data['syzqy'],
                                            data['yysr'])
        cwzb.loc[:,'gdzczzl_1'] = np.divide(
            data['gdzcyj_1'] - data['zj_1'] + data['gdzcyj'] - data['zj'],
            data['yysr'])
        cwzb.loc[:,'chzzl_1'] = np.divide(data['ch_1'] + data['ch'],
                                         data['yysr'])
        cwzb.loc[:,'zclrl'] = np.divide(data['lrze'],
                                       (data['zchj_1'] + data['zchj']) * 0.5)
        cwzb.loc[:,'zzcszl'] = np.divide(data['jlr'], data['zchj'])
        cwzb.loc[:,'zzcjll'] = np.divide(data['jlr'], data['zchj'])
        cwzb.loc[:,'zzcbcl'] = np.divide((data['zchj_1'] + data['zchj']) * 0.5,
                                        (data['zchj_1'] + data['zchj']) * 0.5)
        cwzb.loc[:,'zcjll'] = np.divide(data['jlr'],
                                       (data['zchj_1'] + data['zchj']) * 0.5)
        cwzb.loc[:,'zcjll_1'] = np.divide(
            data['jlr'] - data['yywsr'] + data['yywzc'],
            (data['zchj_1'] + data['zchj']) * 0.5)
        cwzb.loc[:,'sqxsjll'] = np.divide(data['lrze'], data['yysr'])
        cwzb.loc[:,'sqzzcjll'] = np.divide(data['lrze'], data['zchj'])
        cwzb.loc[:,'zzmll'] = np.divide(data['yysr'] - data['yycb'],
                                       data['yysr'])
        cwzb.loc[:,'xsjll'] = np.divide(data['jlr'], data['yysr'])
        cwzb.loc[:,'xsfzbl'] = np.divide(data['xsfy'], data['yysr'])
        cwzb.loc[:,'zzwszbl'] = np.divide(data['yywsr'] - data['yywzc'],
                                         data['lrze'])
        cwzb.loc[:,'tzszbl'] = np.divide(data['tzsy'], data['lrze'])
        cwzb.loc[:,'sxfzbl'] = np.divide(
            data['xsfy'] + data['glfy'] + data['cwfy'], data['yysr'])
        #cwzb.loc[:'qtzwlrbl']=np.divide(data[''])
        cwzb.loc[:,'xsqlrl'] = np.divide(data['lrze'] + data['cwfy'],
                                        data['yysr'])
        cwzb.loc[:,'glfzbl'] = np.divide(data['glfy'], data['yysr'])
        cwzb.loc[:,'zzsrchb'] = np.divide(data['ch'], data['yysr'])
        cwzb.loc[:,'zskzzzcbbl'] = np.divide(data['yszk'], data['yycb'])
        cwzb.loc[:,'chzzzcbbl'] = np.divide(data['ch'], data['yycb'])
        cwzb.loc[:,'zfzkzzzcbbl'] = np.divide(data['yfzk'], data['yycb'])
        cwzb.loc[:,'bwfzbl'] = np.divide(data['cwfy'], data['yysr'])
        cwzb.loc[:,'jzcszlv'] = np.divide(
            data['jlr'], (data['syzqy_1'] + data['syzqy']) * 0.5)
        cwzb.loc[:,'zczzlrlv'] = np.divide(
            data['yylr'], (data['zchj_1'] + data['zchj']) * 0.5)
        cwzb.loc[:,'jzcszl_1'] = np.divide(
            data['yylr'], (data['syzqy_1'] + data['syzqy']) * 0.5)
        cwzb.loc[:,'zzczzlv'] = np.divide(data['zchj'] - data['zchj_1'],
                                         data['zchj_1'])
        cwzb.loc[:,'zzsrzzl'] = np.divide(data['yysr'] - data['yysr_1'],
                                         data['yysr_1'])
        cwzb.loc[:,'zszktbzzl'] = np.divide(data['yszk'] - data['yszk_1'],
                                           data['yszk_1'])
        cwzb.loc[:,'yuszkzzl'] = np.divide(data['yuszk'] - data['yuszk_1'],
                                          data['yuszk_1'])
        cwzb.loc[:,'yszkzzlv'] = np.divide(data['yszk'] - data['yszk_1'],
                                          data['yszk_1'])
        cwzb.loc[:,'zfkzzlv'] = np.divide(
            data['yfzk'] + data['qtyfk'] - data['yfzk_1'] - data['qtyfk_1'],
            data['yfzk_1'] + data['qtyfk_1'])
        cwzb.loc[:,'chtbzzlv'] = np.divide(data['ch'] - data['ch_1'],
                                          data['ch_1'])
        cwzb.loc[:,'zzlrzzlv'] = np.divide(data['yylr'] - data['yylr_1'],
                                          data['yylr_1'])
        cwzb.loc[:,'szzqzzzlv'] = np.divide(data['syzqy'] - data['syzqy_1'],
                                           data['syzqy_1'])
        cwzb.loc[:,'lrzezzlv'] = np.divide(data['lrze'] - data['lrze_1'],
                                          data['lrze_1'])
        cwzb.loc[:,'jzcszlzzlv'] = np.divide(
            (np.divide(data['jlr'], data['syzqy']) - np.divide(
                data['jlr_1'], data['syzqy_1'])),
            np.divide(data['jlr_1'], data['syzqy_1']))
        cwzb.loc[:,'zzwsrzzlv'] = np.divide(
            data['yywsr'] - data['yywzc'] - data['yywsr_1'] + data['yywzc_1'],
            data['yywsr_1'] - data['yywzc_1'])
        cwzb.loc[:,'jlrzzlv'] = np.divide(data['jlr'] - data['jlr_1'],
                                         data['jlr_1'])
        cwzb.loc[:,'gdzczzlv'] = np.divide(data['gdzchj'] - data['gdzchj_1'],
                                          data['gdzchj_1'])
        pprint(cwzb.head())
        pprint(cwzb.info())
        return cwzb

    def get_cwxz_info(self):
        data = self.cw_data
        cwxz_info = DataFrame(index=[0])
        cwxz_info.loc[:, 'yyzbsrb'] = np.divide(
            data['ldzchj'] + data['ldfzhj'], data['yysr'])
        cwxz_info.loc[:, 'srfyb'] = np.divide(
            data['yysr'], data['glfy'] + data['xsfy'] + data['cwfy'])
        cwxz_info.loc[:, 'zzczzl'] = np.divide(
            data['yysr'], (data['zchj'] + data['zchj_1']) * 0.5)
        cwxz_info.loc[:, 'yszkzzl'] = np.divide(
            data['yysr'], (data['yszk'] + data['yszk_1']) * 0.5)
        cwxz_info.loc[:, 'dlzczzl'] = np.divide(
            data['yysr'], (data['ldzchj'] + data['ldzchj_1']) * 0.5)
        cwxz_info.loc[:, 'gdzczzl'] = np.divide(
            data['yysr'], (data['gdzchj'] + data['gdzchj_1']) * 0.5)
        cwxz_info.loc[:, 'yfzkzzl'] = np.divide(
            data['yycb'], (data['yfzk'] + data['yfzk_1']) * 0.5)
        cwxz_info.loc[:, 'yysrzzjkb'] = np.divide(
            data['yysr'], (data['dqjk'] + data['cqjk'] + data['yfzq'] +
                           data['ynndqdcqfz'] + data['dqjk_1'] + data['cqjk_1']
                           + data['yfzq_1'] + data['ynndqdcqfz_1']) * 0.5)
        cwxz_info.loc[:, 'zcfzl'] = np.divide(data['fzhj'], data['zchj'])
        cwxz_info.loc[:, 'ldzcfzl'] = np.divide(data['ldfzhj'], data['zchj'])
        cwxz_info.loc[:, 'fxzwjgb'] = np.divide(
            data['dqjk'] + data['cqjk'] + data['yfzq'], data['fzhj'])
        cwxz_info.loc[:, 'fldfzqyb'] = np.divide(data['fzhj'] - data['ldzchj'],
                                                 data['syzqy'])
        cwxz_info.loc[:, 'zjkqyb'] = np.divide(
            data['dqjk'] + data['cqjk'] + data['yfzq'], data['syzqy'])
        cwxz_info.loc[:, 'jjkqyb'] = np.divide(
            data['dqjk'] + data['cqjk'] + data['yfzq'], data['syzqy'])
        cwxz_info.loc[:, 'tzldfzqyb'] = np.divide(
            data['ldfzhj'], data['syzqy'] - data['cqdtfy'] - data['wxzc'])
        cwxz_info.loc[:, 'lxbzbs'] = np.divide(data['lrze'] + data['cwfy'],
                                               data['cwfy'])
        cwxz_info.loc[:, 'srlxbzbs'] = np.divide(data['yysr'], data['cwfy'])
        cwxz_info.loc[:, 'EBITDAlxbzbs'] = np.divide(
            data['lrze'] + data['cwfy'] + data['zj'], data['cwfy'])
        cwxz_info.loc[:, 'xqyylrdqjkb'] = np.divide(
            data['yysr'] - data['yycb'] - data['yysjjfj'] - data['xsfy'] -
            data['glfy'], (data['dqjk'] + data['cwfy'] + data['ynndqdcqfz'] +
                           data['dqjk_1'] + data['ynndqdcqfz_1']) * 0.5)
        cwxz_info.loc[:, 'EBITdqjkb'] = np.divide(
            data['lrze'] + data['cwfy'],
            (data['dqjk'] + data['ynndqdcqfz'] + data['cwfy'] + data['dqjk_1']
             + data['ynndqdcqfz_1']) * 0.5)
        cwxz_info.loc[:, 'xsqlrfzb'] = np.divide(
            data['lrze'] + data['cwfy'], (data['fzhj'] + data['fzhj_1']) * 0.5)
        cwxz_info.loc[:, 'sqlrzjkb'] = np.divide(
            data['lrze'],
            (data['dqjk'] + data['cqjk'] + data['yfzq'] + data['dqjk_1'] +
             data['cqjk_1'] + data['yfzq_1']) * 0.5)
        cwxz_info.loc[:, 'yylrzjkb'] = np.divide(
            data['yylr'] - data['tzsy'], data['dqjk'] + data['ynndqdcqfz'] +
            (data['cqjk'] + data['yfzq'] + data['dqjk_1'] +
             data['ynndqdcqfz_1'] + data['cqjk_1'] + data['yfzq_1']) * 0.5)
        cwxz_info.loc[:, 'yylrfzb'] = np.divide(
            data['yylr'] - data['tzsy'], (data['fzhj'] + data['fzhj_1']) * 0.5)
        cwxz_info.loc[:, 'yylrjjkb'] = np.divide(
            data['lrze'],
            (data['dqjk'] + data['ynndqdcqfz'] + data['cqjk'] + data['yfzq'] +
             data['yfpj'] - data['hbzj'] + data['dqjk_1'] +
             data['ynndqdcqfz_1'] + data['cqjk_1'] + data['yfzq_1'] +
             data['yfpj_1'] - data['hbzj_1']) * 0.5)
        cwxz_info.loc[:, 'qyzzl'] = np.divide(
            data['syzqy'] - data['syzqy_1'],
            data['syzqy_1'].apply(lambda x: abs(x)))
        cwxz_info.loc[:, 'sdbl'] = np.divide(data['ldzchj'] - data['ch'],
                                             data['ldfzhj'])
        cwxz_info.loc[:, 'ldbl'] = np.divide(data['ldzchj'], data['ldfzhj'])
        cwxz_info.loc[:, 'dqjkxjbl'] = np.divide(
            data['hbzj'], data['dqjk'] + data['ynndqdcqfz'])
        cwxz_info.loc[:, 'sdzczb'] = np.divide(
            data['hbzj'] + data['yfpj'] + data['yszk'], data['zchj'])
        cwxz_info.loc[:, 'mll'] = np.divide(
            data['yysr'] - data['yycb'] - data['yysjjfj'], data['yysr'])
        cwxz_info.loc[:, 'yylrl'] = np.divide(data['yylr'] - data['tzsy'],
                                              data['yysr'])
        cwxz_info.loc[:, 'sqlrl'] = np.divide(data['lrze'], data['yysr'])
        cwxz_info.loc[:, 'zcsyl'] = np.divide(
            data['jlr'], (data['zchj'] + data['zchj_1']) * 0.5)

        pprint(cwxz_info.head())
        return cwxz_info

    def get_concat_df(self):
        self.get_cw_data()
        cwzb_info=self.get_cwzb_info()
        cwxz_info=self.get_cwxz_info()
        concat_df=pd.concat([cwzb_info,cwxz_info],axis=1)
        concat_df['nsrsbh']=self.nsrsbh
        concat_df.to_csv('D:/业务文档/平安银行/cwzb_all.csv')
        print(concat_df)
        return concat_df



if __name__ == '__main__':
    h = handle_cwzb('91350211594998858T')
    h.get_cw_data()
    #h.get_cwzb_info()
    #h.get_cwxz_info()
    #h.get_concat_df()