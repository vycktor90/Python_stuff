#function to get historical 1M data from Oanda
#Copyright Daniel Fernandez 
# http://mechanicalforex.com
# https://asirikuy.com

# This function requires the rfc3339 and requests libraries
# the ending data is corrected to GMT +1/+2

global OandaSymbol
OandaSymbol = {
'EURUSD': 'EUR_USD',
'GBPUSD': 'GBP_USD',
'USDJPY': 'USD_JPY',
'AUDUSD': 'AUD_USD',
'USDCHF': 'USD_CHF',
'USDCAD': 'USD_CAD',
'EURCHF': 'EUR_CHF',
'EURGBP': 'EUR_GBP',
'EURJPY': 'EUR_JPY',
'AUDJPY': 'AUD_JPY',
'AUDCAD': 'AUD_CAD',
'GBPJPY': 'GBP_JPY',
'GBPCHF': 'GBP_CHF',
'GBPAUD': 'GBP_AUD',
'GBPCAD': 'GBP_CAD',
'EURAUD': 'EUR_AUD',
'EURCAD': 'EUR_CAD'
}
def get_historical_data(symbol, endingDate="2015-01-01", tokenValue="3a70a46cbc2becd81f8e560c74768220-d81de0cee8049c3a80a2391d55a9a819"):
    import requests
    import rfc3339
    from datetime import datetime

    print "getting data for {}".format(symbol)
    
    j = 0
    
    while True:
    
        if j > 0:
            date = parser.parse(lastCandleTime[:10]) + datetime.timedelta(hours=10)  
       
            if date < datetime.datetime.strptime(endingDate, "%Y-%m-%d"):
                break

        try:
            token = "Bearer %s" % (tokenValue)
            server = "api-fxpractice.oanda.com"
            headers = {'Authorization': token, 'Connection': 'Keep-Alive',  'Accept-Encoding': 'gzip, deflate', 'Content-type': 'application/x-www-form-urlencoded'}
            
            global OandaSymbol
            
            if j == 0:
                url = "https://" + server + "/v1/candles?instrument=" + OandaSymbol[symbol] + "&count=5000&granularity=M1"
            else:                          
                url = "https://" + server + "/v1/candles?instrument=" + OandaSymbol[symbol] + "&count=5000&granularity=M1&end=" + date.strftime("%Y-%m-%d")
            print "before try"
          
            try:
                req = requests.get(url, headers = headers)
                print "failed 2"
                resp = req.json()
                
            except Exception as e:
                print "failed there"
                print e
           
                print "ERROR"
                
                quit()
            
            candles = resp['candles']   

            ratesTime = []
            ratesOpen = []
            ratesHigh = []
            ratesLow = []
            ratesClose = []
            ratesVolume = []

            i = 0
            
            for candle in reversed(candles):
                print "1"
                try:
                    print candle['time']
                    timestamp =candle['time']   #int(rfc3339.FromTimestamp(candle['time']))
                    print "1.1"
                    correctedTimeStamp = timezone('UTC').localize(datetime.datetime.utcfromtimestamp(timestamp))
                    print "1.2"
                    correctedTimeStamp = correctedTimeStamp.astimezone(timezone('Europe/Madrid'))
                except Exception as e:
                    print e
                
                
                if (correctedTimeStamp.isoweekday() < 6):
                        
                    ratesTime.append(datetime.datetime.utcfromtimestamp((int(calendar.timegm(correctedTimeStamp.timetuple())))))
                    ratesOpen.append(float(candle['openBid']))
                    ratesHigh.append(float(candle['highBid']))
                    ratesLow.append(float(candle['lowBid']))
                    ratesClose.append(float(candle['closeBid']))
                    ratesVolume.append(float(candle['volume']))
                    lastCandleTime = candle['time']
                    i += 1
            print "2"
            ratesFromServer = {"1_Open": ratesOpen,
                               "2_High": ratesHigh,
                               "3_Low": ratesLow,
                               "4_Close": ratesClose,
                               "5_Volume":ratesVolume}
            
            ratesFromServer = pd.DataFrame(ratesFromServer, index=ratesTime)
            print "before if"
            if j == 0:
                print "inside last if"
                finalRates = ratesFromServer
            else:
                finalRates = finalRates.append(ratesFromServer).sort_index()
                #finalRates = finalRates.drop_duplicates(keep='last') #done!
                finalRates = finalRates[~finalRates.index.duplicated(keep='first')]
                
            print "{}, {}".format(finalRates.index[0], lastCandleTime)
                       
            j += 1
        except:
            break
            
    finalRates.to_csv("OANDA_"+symbol + '_1.csv', date_format='%d/%m/%y %H:%M', header = False) 
    
