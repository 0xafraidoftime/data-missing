'''
Id:          "$Id: rateseoddatasources.py,v 1.4 2024/04/23 16:05:17 itisha.gupta Exp $"
Copyright:   Copyright (c) 2023 Bank of America Merrill Lynch, All Rights Reserved
Description: SCENARIO 4: Multiple VTDs missing data - AMRS all measures missing + APAC one measure missing
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
    SCENARIO 4: Multiple VTDs missing data
    - AMRS LINEAR RATES: All measures missing
    - APAC LINEAR RATES: One measure (Sov Spread Delta) missing
    """
    logger.info(f"SCENARIO 4: Processing {name} with source {key}")
    
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
    
    # SCENARIO 4: Handle multiple VTDs with missing data
    if name == "AMRS LINEAR RATES":
        logger.info("=== SCENARIO 4: Simulating AMRS LINEAR RATES - ALL MEASURES MISSING ===")
        # Return empty tables for all measures
        for measure in fieldsDict.get('measure_names',[]):
            rraExpTable = qztable.Table()
            rraExposureTable = qztable.Table()
            snapshots.update({measure: rraExpTable})
            expTable = concatenateExpTables(expTable, rraExposureTable)
            # Mark all measures as missing with source information
            fieldsDict = getMissingMeasures(measuresMissingExposures, f"{measure} missing from [{key}] datasource", fieldsDict)
        return snapshots, expTable, fieldsDict
    
    # Normal processing for all VTDs
    for measure in fieldsDict.get('measure_names',[]):
        # SCENARIO 4: For APAC LINEAR RATES, skip Sov Spread Delta to simulate partial missing data
        if name == "APAC LINEAR RATES" and measure == "Sov Spread Delta":
            logger.info("=== SCENARIO 4: Simulating APAC LINEAR RATES - Sov Spread Delta MISSING ===")
            rraExpTable = qztable.Table()
            rraExposureTable = qztable.Table()
            snapshots.update({measure: rraExpTable})
            expTable = concatenateExpTables(expTable, rraExposureTable)
            # Mark Sov Spread Delta as missing with source information
            fieldsDict = getMissingMeasures(measuresMissingExposures, f"Sov Spread Delta missing from [{key}] datasource", fieldsDict)
            continue
        
        querySet = {}
        querySet.update({'Measure':measure})
        querySet.update(fieldsDict)
        rraExpTable = fetch_exposures_eod(cfg, querySet,filter)
        rraExpTable.renameCol(['_'.join([querySet[MEASURE_COL], 'USD'])],['Exposures_USD'])
        rraExposureTable = rraExpTable.extendConst(measure, MEASURE_COL, 'string')
        
        # Original data clearing conditions
        if measure in ["IR Delta", "IR Vega", "Inflation Delta"] and name == "GLOBAL RATES":
            rraExposureTable = qztable.Table(rraExposureTable.getSchema())
            rraExpTable = qztable.Table(rraExpTable.getSchema())
        if measure in ["IR Delta", "Inflation Delta"] and name == "GLOBAL NON-LINEAR-AMRS STRUCTURED RATES":
            rraExposureTable = qztable.Table(rraExposureTable.getSchema())
            rraExpTable = qztable.Table(rraExpTable.getSchema())
        if measure in ["Inflation Delta"] and name == "GLOBAL NON-LINEAR-EMEA STRUCTURED RATES":
            rraExposureTable = qztable.Table(rraExposureTable.getSchema())
            rraExpTable = qztable.Table(rraExpTable.getSchema())
            
        snapshots.update({measure: rraExpTable})
        expTable = concatenateExpTables(expTable, rraExposureTable)
        
        if not rraExposureTable:
            fieldsDict = getMissingMeasures(measuresMissingExposures, measure, fieldsDict)
    return snapshots, expTable, fieldsDict

def fetchFromLegacy(cfg, key, dataSources, jobTimeStamp, name):
    snapshots = {}
    expTable = None
    measuresMissingExposures = {}
    fieldsDict = createParams(key, dataSources)
    
    # Normal processing for legacy sources
    for measure in fieldsDict.get('measure_names',[]):
        querySet = {}
        querySet.update({'Measure':measure})
        querySet.update(fieldsDict)
        querySet.update(cfg.get('rra_query_params', None))
        querySet['tz'] = jobTimeStamp.tzinfo.zone
        legacyExpTable, expPath = legacy_exposures.fetch(querySet, jobTimeStamp.hour, cfg)
        legacyExposureTable = legacyExpTable.extendConst(measure, MEASURE_COL, 'string')
        
        if measure in ["IRDelta"] and name == "GLOBAL NON-LINEAR-EMEA STRUCTURED RATES":
            legacyExposureTable = qztable.Table(legacyExposureTable.getSchema())
            legacyExpTable = qztable.Table(legacyExpTable.getSchema())
        if measure in ["IR Delta"] and name == "GLOBAL NON-LINEAR-AMRS STRUCTURED RATES":
            legacyExposureTable = qztable.Table(legacyExposureTable.getSchema())
            legacyExpTable = qztable.Table(legacyExpTable.getSchema()) 
            
        snapshots.update({measure: legacyExpTable})
        expTable = concatenateExpTables(expTable, legacyExposureTable)
        
        if not legacyExposureTable:
            fieldsDict = getMissingMeasures(measuresMissingExposures, measure, fieldsDict)
    return snapshots, expTable, fieldsDict

def run(config = 'dev_gnlr_amrs_rates_eod'):
    cfg = CFTCConfStatic(config)
    key = 'legacy'
    dataSources  = {'cirt_rra': [{'measure_names': ['IR Delta', 'IR Vega']}, {'measure_name_overrides': [{'IR Delta': 'IR01'}, {'IR Vega': 'Vega'}]}, {'calc_level': ['VTD']}], 'legacy': [{'measure_names': ['IR Delta', 'IR Vega']}, {'measure_name_overrides': [{'IR Delta': 'CFTC-IRDelta'}, {'IR Vega': 'CFTC-IRVega'}]}, {'legacy_db': 'ficc_reportresults'}, {'legacy_db_path': '/Applications/RemoteRisk/intraday/official/ciro_amrs_rates_positions/CFTC'}]}
    dataSourceFactory(cfg, key, dataSources)
    
def main():
    logging.compliance(__name__, "Bob Run", action=logging.Action.ENTRYPOINT)
    bobfns.run(run)