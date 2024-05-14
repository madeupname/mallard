column_mapping = {
    'tiingo': {
        'symbol': 'ticker',
        'asset_type': 'assetType',
        'price_currency': 'priceCurrency',
        'start_date': 'startDate',
        'end_date': 'endDate',
        'vendor_symbol_id': 'permaTicker',
        'is_active': 'isActive',
        'is_adr': 'isADR',
        'sic_code': 'sicCode',
        'sic_sector': 'sicSector',
        'sic_industry': 'sicIndustry',
        'reporting_currency': 'reportingCurrency',
        'company_website': 'companyWebsite',
        'sec_filing_website': 'secFilingWebsite',
        'statement_last_updated': 'statementLastUpdated',
        'daily_last_updated': 'dailyLastUpdated',
        'vendor_entity_id': 'dataProviderPermaTicker',
        'adj_close': 'adjClose',
        'adj_high': 'adjHigh',
        'adj_low': 'adjLow',
        'adj_open': 'adjOpen',
        'adj_volume': 'adjVolume',
        'div_cash': 'divCash',
        'split_factor': 'splitFactor',
        'market_cap': 'marketCap',
        'enterprise_val': 'enterpriseVal',
        'pe_ratio': 'peRatio',
        'pb_ratio': 'pbRatio',
        'trailing_peg_1y': 'trailingPEG1Y'
    },
}

def get_column_mapper(provider: str):
    def get_provider_name(column: str) -> str:
        if column in column_mapping[provider].keys():
            return column_mapping[provider][column]
        return column
    return get_provider_name

def get_sql_column_provider(provider: str):
    col = get_column_mapper(provider)
    def get_sql_column(column: str) -> str:
        return f"{col(column)} AS '{column}'"
    return get_sql_column
