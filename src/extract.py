import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Acciones que vamos a monitorear
TICKERS = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

def extract_stock_data(tickers: list = TICKERS, days: int = 30) -> pd.DataFrame:
    """
    Extrae precios históricos de Yahoo Finance.
    
    Args:
        tickers: Lista de símbolos bursátiles
        days: Cantidad de días hacia atrás a extraer
    
    Returns:
        DataFrame con los datos crudos
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days)
    
    all_data = []
    
    for ticker in tickers:
        try:
            logger.info(f"Extrayendo datos de {ticker}...")
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date)
            
            if df.empty:
                logger.warning(f"No se obtuvieron datos para {ticker}")
                continue
            
            df = df.reset_index()
            df["ticker"] = ticker
            all_data.append(df)
            logger.info(f"✓ {ticker}: {len(df)} registros extraídos")
            
        except Exception as e:
            logger.error(f"Error extrayendo {ticker}: {e}")
    
    if not all_data:
        raise ValueError("No se pudo extraer datos de ningún ticker")
    
    result = pd.concat(all_data, ignore_index=True)
    logger.info(f"Extracción completada: {len(result)} registros totales")
    return result


if __name__ == "__main__":
    df = extract_stock_data()
    print(df.head())
    print(f"\nColumnas: {df.columns.tolist()}")
    print(f"Shape: {df.shape}")