# Testing Scenarios for EOD Limits Check Report - Enhanced Version
# Add these code snippets in ratesdatasources.py in the dataSourceFactory function
# Modify the data returned to simulate missing data scenarios with source-specific information
# Comment/uncomment the relevant scenario before running each test

# =============================================================================
# SCENARIO 1: All VTDs have data (Baseline - no modifications needed)
# =============================================================================
# This is the default behavior - no code changes required
# Just run the script normally to verify all 8 VTDs generate reports with attachments

# =============================================================================
# SCENARIO 1a: GNLR AMRS VTD with multiple sources (Enhanced)
# =============================================================================
# No code modification needed - this VTD naturally has multiple sources
# Verify in the output that both legacy and cirt_unified_screen data sources are present
# The fieldsDict should contain source information for both data sources

# =============================================================================
# SCENARIO 2a: First VTD (AMRS LINEAR RATES) - all measures missing data
# =============================================================================
def simulate_amrs_all_missing(cfg, sourceKey, dataSources):
    """Simulate all measures missing for AMRS LINEAR RATES"""
    if cfg.get('trading_desk') == 'AMRS LINEAR RATES':
        # Return empty tables but maintain proper fieldsDict structure
        snapshotsForSource = {}
        expTable = qztable.Table()
        
        # Set all measures as missing with source information
        fieldsDict = {
            'source': sourceKey,  # Will be 'cirt_unified_screen'
            'measure_names': ['IR01', 'Vega', 'Sov Spread Delta'], 
            'calc_level': ['VTD+Currency'],
            'measuresMissingExposures': [
                f'IR01 missing from [{sourceKey}]',
                f'Vega missing from [{sourceKey}]', 
                f'Sov Spread Delta missing from [{sourceKey}]'
            ], 
            'level': 'AMRS LINEAR RATES'
        }
        return snapshotsForSource, expTable, fieldsDict
    return None

# =============================================================================
# SCENARIO 2b: Last VTD (GNLR EMEA STRUCTURED NOTES) - all measures missing data  
# =============================================================================
def simulate_gnlr_emea_all_missing(cfg, sourceKey, dataSources):
    """Simulate all measures missing for GNLR EMEA STRUCTURED NOTES"""
    if cfg.get('trading_desk') == 'GLOBAL NON-LINEAR-EMEA STRUCTURED RATES':
        snapshotsForSource = {}
        expTable = qztable.Table()
        
        fieldsDict = {
            'source': sourceKey,  # Will be 'cirt_unified_screen'
            'measure_names': ['IR01', 'Vega'], 
            'calc_level': ['VTD+Currency'],
            'measuresMissingExposures': [
                f'IR01 missing from [{sourceKey}]',
                f'Vega missing from [{sourceKey}]'
            ], 
            'level': 'GLOBAL NON-LINEAR-EMEA STRUCTURED RATES'
        }
        return snapshotsForSource, expTable, fieldsDict
    return None

# =============================================================================
# SCENARIO 2c: Second VTD (APAC LINEAR RATES) - all measures missing data
# =============================================================================
def simulate_apac_all_missing(cfg, sourceKey, dataSources):
    """Simulate all measures missing for APAC LINEAR RATES"""
    if cfg.get('trading_desk') == 'APAC LINEAR RATES':
        snapshotsForSource = {}
        expTable = qztable.Table()
        
        fieldsDict = {
            'source': sourceKey,  # Will be 'cirt_unified_screen'
            'measure_names': ['IR01', 'Sov Spread Delta'], 
            'calc_level': ['VTD+Currency'],
            'measuresMissingExposures': [
                f'IR01 missing from [{sourceKey}]',
                f'Sov Spread Delta missing from [{sourceKey}]'
            ], 
            'level': 'APAC LINEAR RATES'
        }
        return snapshotsForSource, expTable, fieldsDict
    return None

# =============================================================================
# SCENARIO 3a: First VTD (AMRS LINEAR RATES) - one measure missing, others present
# =============================================================================
def simulate_amrs_partial_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict):
    """Simulate Vega missing but other measures present for AMRS LINEAR RATES"""
    if cfg.get('trading_desk') == 'AMRS LINEAR RATES':
        # Keep original data but remove Vega measure
        modifiedSnapshots = {}
        for key, snapshot in originalSnapshotsForSource.items():
            # Filter out Vega data from snapshots
            if 'Measure' in snapshot.columnNames():
                modifiedSnapshots[key] = snapshot[snapshot['Measure'] != 'Vega']
            else:
                modifiedSnapshots[key] = snapshot
        
        # Filter out Vega from exposure table
        modifiedExpTable = originalExpTable
        if originalExpTable and 'Measure' in originalExpTable.columnNames():
            modifiedExpTable = originalExpTable[originalExpTable['Measure'] != 'Vega']
        
        # Update fieldsDict to reflect only Vega missing
        fieldsDict = originalFieldsDict.copy()
        fieldsDict.update({
            'measuresMissingExposures': [f'Vega missing from [{sourceKey}]']
        })
        
        return modifiedSnapshots, modifiedExpTable, fieldsDict
    return None

# =============================================================================
# SCENARIO 3b: Last VTD (GNLR EMEA STRUCTURED NOTES) - one measure missing
# =============================================================================
def simulate_gnlr_emea_partial_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict):
    """Simulate Vega missing but IR01 present for GNLR EMEA STRUCTURED NOTES"""
    if cfg.get('trading_desk') == 'GLOBAL NON-LINEAR-EMEA STRUCTURED RATES':
        # Keep IR01 data but remove Vega
        modifiedSnapshots = {}
        for key, snapshot in originalSnapshotsForSource.items():
            if 'Measure' in snapshot.columnNames():
                modifiedSnapshots[key] = snapshot[snapshot['Measure'] != 'Vega']
            else:
                modifiedSnapshots[key] = snapshot
        
        modifiedExpTable = originalExpTable
        if originalExpTable and 'Measure' in originalExpTable.columnNames():
            modifiedExpTable = originalExpTable[originalExpTable['Measure'] != 'Vega']
        
        fieldsDict = originalFieldsDict.copy()
        fieldsDict.update({
            'measuresMissingExposures': [f'Vega missing from [{sourceKey}]']
        })
        
        return modifiedSnapshots, modifiedExpTable, fieldsDict
    return None

# =============================================================================
# SCENARIO 3c: Second VTD (APAC LINEAR RATES) - one measure missing
# =============================================================================
def simulate_apac_partial_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict):
    """Simulate Sov Spread Delta missing but IR01 present for APAC LINEAR RATES"""
    if cfg.get('trading_desk') == 'APAC LINEAR RATES':
        # Keep IR01 data but remove Sov Spread Delta
        modifiedSnapshots = {}
        for key, snapshot in originalSnapshotsForSource.items():
            if 'Measure' in snapshot.columnNames():
                modifiedSnapshots[key] = snapshot[snapshot['Measure'] != 'Sov Spread Delta']
            else:
                modifiedSnapshots[key] = snapshot
        
        modifiedExpTable = originalExpTable
        if originalExpTable and 'Measure' in originalExpTable.columnNames():
            modifiedExpTable = originalExpTable[originalExpTable['Measure'] != 'Sov Spread Delta']
        
        fieldsDict = originalFieldsDict.copy()
        fieldsDict.update({
            'measuresMissingExposures': [f'Sov Spread Delta missing from [{sourceKey}]']
        })
        
        return modifiedSnapshots, modifiedExpTable, fieldsDict
    return None

# =============================================================================
# SCENARIO 4: Multiple VTDs missing data (Enhanced with source information)
# One VTD all measures missing + one VTD one measure missing
# =============================================================================
def simulate_multiple_vtds_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict):
    """Simulate AMRS all missing + APAC one missing"""
    
    # AMRS - All measures missing
    if cfg.get('trading_desk') == 'AMRS LINEAR RATES':
        snapshotsForSource = {}
        expTable = qztable.Table()
        
        fieldsDict = {
            'source': sourceKey,
            'measure_names': ['IR01', 'Vega', 'Sov Spread Delta'], 
            'calc_level': ['VTD+Currency'],
            'measuresMissingExposures': [
                f'IR01 missing from [{sourceKey}]',
                f'Vega missing from [{sourceKey}]',
                f'Sov Spread Delta missing from [{sourceKey}]'
            ], 
            'level': 'AMRS LINEAR RATES'
        }
        return snapshotsForSource, expTable, fieldsDict
    
    # APAC - One measure missing
    elif cfg.get('trading_desk') == 'APAC LINEAR RATES':
        modifiedSnapshots = {}
        for key, snapshot in originalSnapshotsForSource.items():
            if 'Measure' in snapshot.columnNames():
                modifiedSnapshots[key] = snapshot[snapshot['Measure'] != 'Sov Spread Delta']
            else:
                modifiedSnapshots[key] = snapshot
        
        modifiedExpTable = originalExpTable
        if originalExpTable and 'Measure' in originalExpTable.columnNames():
            modifiedExpTable = originalExpTable[originalExpTable['Measure'] != 'Sov Spread Delta']
        
        fieldsDict = originalFieldsDict.copy()
        fieldsDict.update({
            'measuresMissingExposures': [f'Sov Spread Delta missing from [{sourceKey}]']
        })
        
        return modifiedSnapshots, modifiedExpTable, fieldsDict
    
    return None

# =============================================================================
# SCENARIO 5: GNLR AMRS Multiple Sources - Enhanced Testing
# One source missing specific measures, other source has partial data
# =============================================================================
def simulate_gnlr_amrs_multi_source_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict):
    """Enhanced testing for GNLR AMRS with multiple sources"""
    if cfg.get('trading_desk') == 'GLOBAL NON-LINEAR-AMRS STRUCTURED RATES':
        
        # Legacy source - simulate IR Delta missing
        if sourceKey == 'legacy':
            modifiedSnapshots = {}
            for key, snapshot in originalSnapshotsForSource.items():
                if 'Measure' in snapshot.columnNames():
                    # Remove CFTC-IRDelta (IR01 equivalent in legacy)
                    modifiedSnapshots[key] = snapshot[snapshot['Measure'] != 'CFTC-IRDelta']
                else:
                    modifiedSnapshots[key] = snapshot
            
            modifiedExpTable = originalExpTable
            if originalExpTable and 'Measure' in originalExpTable.columnNames():
                modifiedExpTable = originalExpTable[originalExpTable['Measure'] != 'IR01']
            
            fieldsDict = originalFieldsDict.copy()
            fieldsDict.update({
                'measuresMissingExposures': [f'IR Delta missing from [legacy] datasource']
            })
            
            return modifiedSnapshots, modifiedExpTable, fieldsDict
        
        # CIRT source - simulate Vega missing
        elif sourceKey == 'cirt_unified_screen':
            modifiedSnapshots = {}
            for key, snapshot in originalSnapshotsForSource.items():
                if 'Measure' in snapshot.columnNames():
                    modifiedSnapshots[key] = snapshot[snapshot['Measure'] != 'Vega']
                else:
                    modifiedSnapshots[key] = snapshot
            
            modifiedExpTable = originalExpTable
            if originalExpTable and 'Measure' in originalExpTable.columnNames():
                modifiedExpTable = originalExpTable[originalExpTable['Measure'] != 'Vega']
            
            fieldsDict = originalFieldsDict.copy()
            fieldsDict.update({
                'measuresMissingExposures': [f'Vega missing from [cirt_rra] datasource']
            })
            
            return modifiedSnapshots, modifiedExpTable, fieldsDict
    
    return None

# =============================================================================
# SCENARIO 6: Complete Data Source Failure
# Simulate when one entire data source is unavailable
# =============================================================================
def simulate_complete_source_failure(cfg, sourceKey, dataSources):
    """Simulate complete failure of a data source"""
    if cfg.get('trading_desk') == 'GLOBAL NON-LINEAR-AMRS STRUCTURED RATES' and sourceKey == 'legacy':
        # Simulate legacy source completely unavailable
        snapshotsForSource = {}
        expTable = qztable.Table()
        
        # All legacy measures are missing
        fieldsDict = {
            'source': sourceKey,
            'measure_names': ['CFTC-IRDelta', 'CFTC-IRVega', 'CFTC-IRVegaM1'], 
            'calc_level': ['VTD+Currency'],
            'measuresMissingExposures': [
                'IR Delta missing from [legacy] datasource',
                'IR Vega missing from [legacy] datasource', 
                'Inflation Delta missing from [legacy] datasource'
            ], 
            'level': 'GLOBAL NON-LINEAR-AMRS STRUCTURED RATES'
        }
        return snapshotsForSource, expTable, fieldsDict
    return None

# =============================================================================
# MAIN MODIFICATION TO dataSourceFactory FUNCTION
# =============================================================================
"""
Add this code at the beginning of the dataSourceFactory function in ratesdatasources.py:

def dataSourceFactory(cfg, sourceKey, dataSources, jobTimestamp):
    '''
    Original function implementation...
    '''
    
    # ========= TEST SCENARIO ACTIVATION =========
    # Uncomment ONE scenario at a time for testing
    
    # SCENARIO 2a: AMRS all missing
    # result = simulate_amrs_all_missing(cfg, sourceKey, dataSources)
    # if result: return result
    
    # SCENARIO 2b: GNLR EMEA all missing  
    # result = simulate_gnlr_emea_all_missing(cfg, sourceKey, dataSources)
    # if result: return result
    
    # SCENARIO 2c: APAC all missing
    # result = simulate_apac_all_missing(cfg, sourceKey, dataSources)
    # if result: return result
    
    # For partial scenarios, we need to call the original function first
    # then modify the results
    
    # Call original dataSourceFactory logic here to get baseline data
    # originalSnapshotsForSource, originalExpTable, originalFieldsDict = [original implementation]
    
    # SCENARIO 3a: AMRS partial missing
    # result = simulate_amrs_partial_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict)
    # if result: return result
    
    # SCENARIO 3b: GNLR EMEA partial missing
    # result = simulate_gnlr_emea_partial_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict)
    # if result: return result
    
    # SCENARIO 3c: APAC partial missing
    # result = simulate_apac_partial_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict)
    # if result: return result
    
    # SCENARIO 4: Multiple VTDs missing
    # result = simulate_multiple_vtds_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict)
    # if result: return result
    
    # SCENARIO 5: GNLR AMRS multi-source missing
    # result = simulate_gnlr_amrs_multi_source_missing(cfg, sourceKey, dataSources, originalSnapshotsForSource, originalExpTable, originalFieldsDict)
    # if result: return result
    
    # SCENARIO 6: Complete source failure
    # result = simulate_complete_source_failure(cfg, sourceKey, dataSources)
    # if result: return result
    
    # If no test scenario is active, return original results
    # return originalSnapshotsForSource, originalExpTable, originalFieldsDict
"""

# =============================================================================
# TESTING INSTRUCTIONS
# =============================================================================
"""
To test each scenario:

1. Make a backup of your original ratesdatasources.py file

2. Add the simulation functions to ratesdatasources.py

3. Modify the dataSourceFactory function to include the test scenario checks
   as shown in the MAIN MODIFICATION section above

4. For each scenario, uncomment ONLY the relevant scenario code snippet

5. Run the EOD Limits job from Bob monitor

6. Verify the expected outcomes:

   SCENARIO 1 (All VTDs have data):
   - One "EOD limit based check" email with all 8 VTDs
   - Attachments for all 8 VTDs present
   - No "[CFTC Error][Action Required] MissingExposures:" emails

   SCENARIO 1a (GNLR AMRS multiple sources):
   - Verify both legacy and cirt_unified_screen data in the report
   - Check that data is not duplicated
   - Verify source information is correctly tracked

   SCENARIO 2a/2b/2c (One VTD all measures missing):
   - One "EOD limit based check" email with 7 VTDs (missing VTD excluded)
   - One "[CFTC Error][Action Required] MissingExposures:" email with format:
     "MeasureName missing from [SourceName] datasource"
   - Attachments only for the 7 VTDs with data

   SCENARIO 3a/3b/3c (One VTD one measure missing):
   - One "EOD limit based check" email with all 8 VTDs
   - One "[CFTC Error][Action Required] MissingExposures:" email with format:
     "MeasureName missing from [SourceName] datasource"
   - Attachments for all 8 VTDs with partial data for affected VTD

   SCENARIO 4 (Multiple VTDs missing data):
   - One "EOD limit based check" email with remaining VTDs
   - Multiple "[CFTC Error][Action Required] MissingExposures:" emails
   - Each alert specifies source information

   SCENARIO 5 (GNLR AMRS multi-source enhanced):
   - Alerts should specify which measures are missing from which sources
   - Format: "IR Delta missing from [legacy] datasource" and 
            "Vega missing from [cirt_rra] datasource"

   SCENARIO 6 (Complete source failure):
   - Alert should indicate entire source unavailable
   - All measures from that source should be listed as missing

7. Key verification points:
   - Email subject follows pattern: [CFTC Error][Action Required] MissingExposures: VTD_NAME measures MEASURE_LIST...
   - Email body contains specific source information for each missing measure
   - Attachments are correctly included/excluded based on data availability
   - No duplicate data in reports
   - Log messages confirm proper error handling

8. After testing each scenario, restore the original file before testing the next

9. Document results and verify they match expected behavior
"""