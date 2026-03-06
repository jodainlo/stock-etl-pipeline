import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def transform_stock_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Limpia los datos crudos y calcula métricas financieras.
    
    Args:
        df: DataFrame crudo de extract.py
    
    Returns:
        Tupla con (precios_limpios, métricas_calculadas)
    """
    logger.info("Iniciando transformacion de datos...")
    
    # --- 1. LIMPIEZA BÁSICA ---
    df_clean = df.copy()
    
    # Estandarizar nombres de columnas
    df_clean.columns = [col.lower().replace(" ", "_") for col in df_clean.columns]
    
    # Columnas que necesitamos
    columns_map = {
        "date": "date",
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
        "volume": "volume",
        "ticker": "ticker"
    }
    
    df_clean = df_clean.rename(columns=columns_map)
    
    # Quedarnos solo con columnas relevantes
    keep_cols = ["ticker", "date", "open_price", "high_price",
                 "low_price", "close_price", "volume"]
    df_clean = df_clean[[c for c in keep_cols if c in df_clean.columns]]
    
    # Limpiar la columna date (quitar timezone)
    df_clean["date"] = pd.to_datetime(df_clean["date"]).dt.date
    
    # Eliminar duplicados
    df_clean = df_clean.drop_duplicates(subset=["ticker", "date"])
    
    # Eliminar filas con precios nulos
    df_clean = df_clean.dropna(subset=["close_price"])
    
    # Asegurar tipos de datos correctos
    numeric_cols = ["open_price", "high_price", "low_price", "close_price"]
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").round(4)
    
    df_clean["volume"] = df_clean["volume"].fillna(0).astype(int)
    
    logger.info(f"Limpieza completada: {len(df_clean)} registros válidos")
    
    # --- 2. CÁLCULO DE MÉTRICAS ---
    metrics_list = []
    
    for ticker in df_clean["ticker"].unique():
        df_ticker = df_clean[df_clean["ticker"] == ticker].copy()
        df_ticker = df_ticker.sort_values("date")
        
        # Retorno diario porcentual
        df_ticker["daily_return"] = df_ticker["close_price"].pct_change().round(6)
        
        # Medias móviles
        df_ticker["ma_7"]  = df_ticker["close_price"].rolling(window=7).mean().round(4)
        df_ticker["ma_30"] = df_ticker["close_price"].rolling(window=30).mean().round(4)
        
        # Volatilidad (desviación estándar de retornos en 7 días)
        df_ticker["volatility_7"] = df_ticker["daily_return"].rolling(window=7).std().round(6)
        
        metrics_list.append(df_ticker[["ticker", "date", "daily_return",
                                        "ma_7", "ma_30", "volatility_7"]])
    
    df_metrics = pd.concat(metrics_list, ignore_index=True)
    df_metrics = df_metrics.dropna(subset=["daily_return"])
    
    logger.info(f"Metricas calculadas: {len(df_metrics)} registros")
    
    return df_clean, df_metrics


if __name__ == "__main__":
    from extract import extract_stock_data
    raw = extract_stock_data(days=30)
    prices, metrics = transform_stock_data(raw)
    print("\n--- PRECIOS LIMPIOS ---")
    print(prices.head())
    print("\n--- MÉTRICAS ---")
    print(metrics.head(10))