'''
Id:          "$Id: rateseoddatasources.py,v 1.4 2024/04/23 16:05:17 itisha.gupta Exp $"
Copyright:   Copyright (c) 2023 Bank of America Merrill Lynch, All Rights Reserved
Description: SCENARIO 2c: APAC LINEAR RATES - All measures missing
Test:
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

logger = logging.getLogger(__name__)

def dataSourceFactory(cfg, key, dataSources, jobTimeStamp, name):
    """
    SCENARIO 2c: APAC LINEAR RATES - All measures missing
    Simulate missing data for APAC LINEAR RATES VTD
    """
    logger.info(f"SCENARIO 2c: Processing {name} with source {key}")
    
    if key in ['management_rra', 'cirt_rra']:
        return fetchFromRRA(cfg, key, dataSources, jobTimeStamp, name)
    if key == 'legacy':
        return fetchFromLegacy(cfg, key, dataSources, jobTimeStamp, name)

def createFilter(cfg):
    filter = Where('DivisionName')==cfg.get('division', 'FICC')
    for k, v in qzsix.iteritems(cfg['rra_query_params']):
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
    measuresMissingExposures.update({measure:[fieldsDict['source']]})
    fieldsDict.update({'measuresMissingExposures': measuresMissingExposures})
    return fieldsDict

def fetchFromRRA(cfg, key, dataSources, jobTimeStamp, name):
    snapshots = {}
    expTable = None
    measuresMissingExposures = {}
    fieldsDict = createParams(key, dataSources)
    filter = createFilter(cfg)
    
    # SCENARIO 2c: Force APAC LINEAR RATES to have no data
    if name == "APAC LINEAR RATES":
        logger.info("=== SCENARIO 2c: Simulating APAC LINEAR RATES - ALL MEASURES MISSING ===")
        # Return empty tables for all measures (IR01, Sov Spread Delta)
        for measure in fieldsDict.get('measure_names',[]):
            rraExpTable = qztable.Table()
            rraExposureTable = qztable.Table()
            snapshots.update({measure: rraExpTable})
            expTable = concatenateExpTables(expTable, rraExposureTable)
            # Mark all measures as missing with source information
            fieldsDict = getMissingMeasures(measuresMissingExposures, f"{measure} missing from [{key}] datasource", fieldsDict)
        return snapshots, expTable, fieldsDict
    
    # Normal processing for other VTDs
    for measure in fieldsDict.get('measure_names',[]):
        querySet = {}
        querySet.update({'Measure':measure})
        querySet.update(fieldsDict)
        rraExpTable = fetch_exposures_eod(cfg, querySet,filter)
        rraExpTable.renameCol(['_'.join([querySet[MEASURE_COL], 'USD'])],['Exposures_USD'])
        rraExposureTable = rraExpTable.extendConst(measure, MEASURE_COL, 'string')
        
        #