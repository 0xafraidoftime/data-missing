'''
Id:          "$Id: rateseoddatasources.py,v 1.4 2024/04/23 16:05:17 itisha.gupta Exp $"
Copyright:   Copyright (c) 2023 Bank of America Merrill Lynch, All Rights Reserved
Description: SCENARIO 6: Complete Data Source Failure - Legacy source completely unavailable for GNLR AMRS
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
    SCENARIO 6: Complete Data Source Failure
    Simulate complete failure of legacy data source for GNLR AMRS
    All measures from legacy source should be reported as missing
    """
    logger.info(f"SCENARIO 6: Processing {name} with source {key}")
    
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
    
    # Normal processing for RRA sources (CIRT sources should work normally)
    for measure in fieldsDict.get('measure_names',[]):
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
    
    # SCENARIO 6: Simulate complete legacy source failure for GNLR AMRS
    if name == "GLOBAL NON-LINEAR-AMRS STRUCTURED RATES":
        logger.info("=== SCENARIO 6: Simulating COMPLETE LEGACY SOURCE FAILURE for GNLR AMRS ===")
        # Return empty tables for all legacy measures
        for measure in fieldsDict.get('measure_names',[]):
            legacyExpTable = qztable.Table()
            legacyExposureTable = qztable.Table()
            snapshots.update({measure: legacyExpTable})
            expTable = concatenateExpTables(expTable, legacyExposureTable)
            
            # Map legacy measure names to standard names for error reporting
            if measure == "CFTC-IRDelta":
                missing_measure_name = "IR Delta missing from [legacy] datasource"
            elif measure == "CFTC-IRVega":
                missing_measure_name = "IR Vega missing from [legacy] datasource"
            elif measure == "CFTC-IRVegaM1":
                missing_measure_name = "Inflation Delta missing from [legacy] datasource"
            else:
                missing_measure_name = f"{measure} missing from [legacy] datasource"
            
            fieldsDict = getMissingMeasures(measuresMissingExposures, missing_measure_name, fieldsDict)
        return snapshots, expTable, fieldsDict
    
    # Normal processing for other VTDs with legacy sources
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