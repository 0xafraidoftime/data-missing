'''
Id:          "$Id: rateseoddatasources.py,v 1.5 2025/09/01 07:23:58 pavuluri.nikhilsai Exp $"
Copyright:   Copyright (c) 2023 Bank of America Merrill Lynch, All Rights Reserved
Description:
Test: qz.remoterisk.tests.unittests.cftc.limits.rateseoddatasources
'''
import qzsix
import qztable


from qz.core import bobfns
from qz.tools.gov.lib import logging
from qz.remoterisk.cftc.utils.config import CFTCConfStatic
from qz.data.where import Where
from qz.remoterisk.cftc.limits import legacy_exposures
from qz.remoterisk.cftc.limits.utils import concatenateExpTables
from qz.remoterisk.cftc.risk.intraday import fetch_exposures_eod
from qz.remoterisk.cftc.utils.persistence import MEASURE_COL
    
    
def dataSourceFactory(cfg, key, dataSources, jobTimeStamp, name):
    if key in ['management_rra', 'cirt_rra']:
        return fetchFromRRA(cfg, key, dataSources, jobTimeStamp, name)
    if key == 'legacy':
        return fetchFromLegacy(cfg, key, dataSources, jobTimeStamp, name)

def createFilter(cfg):
    filter = Where('DivisionName')==cfg.get('division', 'FICC')
    for k, v in qzsix.iteritems(cfg['rra_query_params']):
        # Use BusinessArea and VolckerTradingDesk names to build the filter.
        if k in ['VolckerBusinessArea', 'VolckerTradingDesk']:
            filter = filter & (Where(k) == v) 
            
    return filter

def createParams(key, dataSources):
    fieldsDict={}
    fieldsDict.update({'source':key})
    for field in dataSources.get(key, None):
        fieldsDict.update(field)
    return fieldsDict

def getMissingMeasures(measuresMissingExposures, measure, fieldsDict):
    '''
    Create {measure:source} dict in case exposures of a measure is missing, and the datasource from which the measure is missing.
    
    :param dict measuresMissingExposures: existing missingMeasures dict
    :param str measure: measure with no exposures to be added to missingMeasures dict
    :param dict fieldsDict: dict with all information related to VTD
    :returns: updated fieldsDict with new missingMeasures
    :rtype: dict
    '''
    measuresMissingExposures.update({measure:[fieldsDict['source']]})
    fieldsDict.update({'measuresMissingExposures': measuresMissingExposures})
    return fieldsDict

def fetchFromRRA(cfg, key, dataSources, jobTimeStamp, name):
    snapshots = {}
    expTable = None
    measuresMissingExposures = {}
    fieldsDict = createParams(key, dataSources)
    filter = createFilter(cfg)
    
    for measure in fieldsDict.get('measure_names',[]):
        querySet = {}
        querySet.update({'Measure':measure})
        querySet.update(fieldsDict)
        measureExpTable = fetch_exposures_eod(cfg, querySet,filter)
        measureExpTable.renameCol(['_'.join([querySet[MEASURE_COL], 'USD'])],['Exposures_USD'])
        measureExposureTable = measureExpTable.extendConst(measure, MEASURE_COL, 'string')
        # if measure in ["IR Delta", "IR Vega"] and name == "GLOBAL RATES":
        #     measureExposureTable = qztable.Table(measureExposureTable.getSchema())
        #     measureExpTable = qztable.Table(measureExpTable.getSchema())
        snapshots.update({measure: measureExpTable})
        expTable = concatenateExpTables(expTable, measureExposureTable)
        
        #adding missing measures for which exposures are empty.
        if not measureExposureTable:
            fieldsDict = getMissingMeasures(measuresMissingExposures, measure, fieldsDict)
    return snapshots, expTable, fieldsDict
        
def fetchFromLegacy(cfg, key, dataSources, jobTimeStamp, name):
    snapshots = {}
    expTable = None
    measuresMissingExposures = {}
    fieldsDict = createParams(key, dataSources)
    
    for measure in fieldsDict.get('measure_names',[]):
        querySet = {}
        querySet.update({'Measure':measure})
        querySet.update(fieldsDict)
        querySet.update(cfg.get('rra_query_params', None))
        querySet['tz'] = jobTimeStamp.tzinfo.zone
        measureExpTable, expPath = legacy_exposures.fetch(querySet, jobTimeStamp.hour, cfg)
        #adding missing measures for which exposures are empty.
        if not measureExpTable:
            fieldsDict = getMissingMeasures(measuresMissingExposures, measure, fieldsDict)
            continue
        measureExposureTable = measureExpTable.extendConst(measure, MEASURE_COL, 'string')
        # if measure in ["IR Delta", "IR Vega", "Inflation Delta"] and name == "GLOBAL RATES":
        #     measureExposureTable = qztable.Table(measureExposureTable.getSchema())
        #     measureExpTable = qztable.Table(measureExpTable.getSchema())
        snapshots.update({measure: measureExpTable})
        expTable = concatenateExpTables(expTable, measureExposureTable)
        
    return snapshots, expTable, fieldsDict
    
def run(config = 'dev_gnlr_amrs_rates_eod'):
    cfg = CFTCConfStatic(config)
    key = 'legacy'
    dataSources  = {'cirt_rra': [{'measure_names': ['IR Delta', 'IR Vega']}, {'measure_name_overrides': [{'IR Delta': 'IR01'}, {'IR Vega': 'Vega'}]}, {'calc_level': ['VTD']}], 'legacy': [{'measure_names': ['IR Delta', 'IR Vega']}, {'measure_name_overrides': [{'IR Delta': 'CFTC-IRDelta'}, {'IR Vega': 'CFTC-IRVega'}]}, {'legacy_db': 'ficc_reportresults'}, {'legacy_db_path': '/Applications/RemoteRisk/intraday/official/ciro_amrs_rates_positions/CFTC'}]}
    dataSourceFactory(cfg, key, dataSources)
    
def main():
    logging.compliance(__name__, "Bob Run", action=logging.Action.ENTRYPOINT)
    bobfns.run(run)
