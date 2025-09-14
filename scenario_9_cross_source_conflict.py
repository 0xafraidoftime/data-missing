'''
Id:          "$Id: rateseoddatasources.py,v 1.4 2024/04/23 16:05:17 itisha.gupta Exp $"
Copyright:   Copyright (c) 2023 Bank of America Merrill Lynch, All Rights Reserved
Description: SCENARIO 13: Cross-Source Measure Conflict - Same measure from multiple sources
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
    SCENARIO 13: Cross-Source Measure Conflict
    Tests handling when the same measure is available from multiple sources
    - GNLR AMRS: IR Vega from both legacy and cirt_unified_screen sources
    - System should handle deduplication and source prioritization
    """
    logger.info(f"SCENARIO 13: Processing {name} with source {key}")
    
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
    
    # Normal processing for all VTDs
    for measure in fieldsDict.get('measure_names',[]):
        # SCENARIO 13: For GNLR AMRS, ensure IR Vega is available from CIRT source
        if name == "GLOBAL NON-LINEAR-AMRS STRUCTURED RATES" and measure in ["Vega", "IR Vega"]:
            logger.info(f"=== SCENARIO 13: GNLR AMRS {measure} available from CIRT source ===")
            # Create mock data to simulate IR Vega from CIRT source
            mockSchema = qztable.TableSchema([
                ('BusinessArea', 'string'),
                ('TradingDesk', 'string'), 
                ('LETier1', 'string'),
                ('Currency', 'string'),
                ('Exposures_USD', 'double')
            ])
            rraExpTable = qztable.Table(mockSchema)
            rraExpTable = rraExpTable.appendRow('GLOBAL RATES', 'GLOBAL NON-LINEAR-AMRS STRUCTURED RATES', 'BKAC', 'USD', 1500000.0)
            
            rraExposureTable = rraExpTable.extendConst(measure, MEASURE_COL, 'string')
            logger.info(f"Mock CIRT data created for {measure}: {rraExposureTable.numRows()} rows")
        else:
            querySet = {}
            querySet.update({'Measure':measure})
            querySet.update(fieldsDict)
            rraExpTable = fetch_exposures_eod(cfg, querySet,filter)
            rraExpTable.renameCol(['_'.join([querySet[MEASURE_COL], 'USD'])],['Exposures_USD'])
            rraExposureTable = rraExpTable.extendConst(measure, MEASURE_COL, 'string')
        
        # Original data clearing conditions (modified for testing)
        if measure in ["IR Delta", "IR Vega", "Inflation Delta"] and name == "GLOBAL RATES":
            rraExposureTable = qztable.Table(rraExposureTable.getSchema())
            rraExpTable = qztable.Table(rraExpTable.getSchema())
        # Allow IR Vega for GNLR AMRS in this conflict scenario
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
    
    for measure in fieldsDict.get('measure_names',[]):
        # SCENARIO 13: For GNLR AMRS, create conflicting IR Vega data from legacy source
        if name == "GLOBAL NON-LINEAR-AMRS STRUCTURED RATES" and measure == "CFTC-IRVega":
            logger.info(f"=== SCENARIO 13: GNLR AMRS IR Vega available from Legacy source (potential conflict) ===")
            # Create mock legacy data to simulate IR Vega conflict
            mockSchema = qztable.TableSchema([
                ('BusinessArea', 'string'),
                ('TradingDesk', 'string'), 
                ('LETier1', 'string'),
                ('Currency', 'string'),
                ('Exposures_USD', 'double')
            ])
            legacyExpTable = qztable.Table(mockSchema)
            # Different value to create conflict with CIRT source
            legacyExpTable = legacyExpTable.appendRow('GLOBAL RATES', 'GLOBAL NON-LINEAR-AMRS STRUCTURED RATES', 'BKAC', 'USD', 1800000.0)
            
            legacyExposureTable = legacyExpTable.extendConst(measure, MEASURE_COL, 'string')
            logger.info(f"Mock Legacy data created for {measure}: {legacyExposureTable.numRows()} rows")
        else:
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
        # Allow CFTC-IRVega for conflict testing
        # if measure in ["IR Delta"] and name == "GLOBAL NON-LINEAR-AMRS STRUCTURED RATES":
        #     legacyExposureTable = qztable.Table(legacyExposureTable.getSchema())
        #     legacyExpTable = qztable.Table(legacyExpTable.getSchema()) 
            
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