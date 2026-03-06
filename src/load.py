import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

load_dotenv()
logger = logging.getLogger(__name__)

def get_engine():
    """Crea la conexión a PostgreSQL usando variables de entorno."""
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5433")
    dbname   = os.getenv("DB_NAME", "stock_pipeline")
    user     = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "12345678")
    
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(connection_string)


def load_to_postgres(df_prices: pd.DataFrame,
                     df_metrics: pd.DataFrame) -> int:
    """
    Carga los datos transformados a PostgreSQL.
    Usa INSERT ... ON CONFLICT DO NOTHING para evitar duplicados.
    
    Returns:
        Número de registros cargados
    """
    engine = get_engine()
    records_loaded = 0
    
    try:
        with engine.connect() as conn:
            
            # --- Cargar precios ---
            logger.info("Cargando tabla stock_prices...")
            for _, row in df_prices.iterrows():
                sql = text("""
                    INSERT INTO stock_prices 
                        (ticker, date, open_price, high_price, low_price, close_price, volume)
                    VALUES 
                        (:ticker, :date, :open_price, :high_price, :low_price, :close_price, :volume)
                    ON CONFLICT (ticker, date) DO NOTHING
                """)
                conn.execute(sql, row.to_dict())
                records_loaded += 1
            
            # --- Cargar métricas ---
            logger.info("Cargando tabla stock_metrics...")
            for _, row in df_metrics.iterrows():
                row_dict = row.where(pd.notnull(row), None).to_dict()
                sql = text("""
                    INSERT INTO stock_metrics
                        (ticker, date, daily_return, ma_7, ma_30, volatility_7)
                    VALUES
                        (:ticker, :date, :daily_return, :ma_7, :ma_30, :volatility_7)
                    ON CONFLICT (ticker, date) DO NOTHING
                """)
                conn.execute(sql, row_dict)
            
            conn.commit()
            logger.info(f"Carga completada: {records_loaded} registros")
    
    except Exception as e:
        logger.error(f"Error en la carga: {e}")
        raise
    
    return records_loaded


def log_execution(status: str, records: int, message: str = ""):
    """Registra cada ejecución del pipeline en la base de datos."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            sql = text("""
                INSERT INTO pipeline_logs (status, records_loaded, message)
                VALUES (:status, :records, :message)
            """)
            conn.execute(sql, {"status": status, "records": records, "message": message})
            conn.commit()
    except Exception as e:
        logger.error(f"Error guardando log: {e}")


def generate_report(output_path: str = "reports/"):
    """Genera un reporte CSV con un resumen de los últimos datos."""
    engine = get_engine()
    
    query = """
        SELECT 
            p.ticker,
            p.date,
            p.close_price,
            m.daily_return,
            m.ma_7,
            m.ma_30,
            m.volatility_7
        FROM stock_prices p
        LEFT JOIN stock_metrics m ON p.ticker = m.ticker AND p.date = m.date
        ORDER BY p.ticker, p.date DESC
    """
    
    df_report = pd.read_sql(query, engine)
    
    filename = f"{output_path}report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_report.to_csv(filename, index=False)
    logger.info(f"Reporte generado: {filename}")
    return filename