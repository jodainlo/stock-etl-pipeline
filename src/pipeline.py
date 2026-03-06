import logging
import sys
import os
from datetime import datetime

# Asegurarnos de que Python encuentre los módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract import extract_stock_data
from src.transform import transform_stock_data
from src.load import load_to_postgres, log_execution, generate_report

# Configurar logging a archivo y consola
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Ejecuta el pipeline ETL completo."""
    logger.info("=" * 50)
    logger.info("INICIANDO PIPELINE ETL - STOCK PRICES")
    logger.info("=" * 50)
    
    try:
        # EXTRACT
        logger.info("[ FASE 1 ] Extracción de datos...")
        raw_data = extract_stock_data(days=30)
        
        # TRANSFORM
        logger.info("[ FASE 2 ] Transformación y limpieza...")
        df_prices, df_metrics = transform_stock_data(raw_data)
        
        # LOAD
        logger.info("[ FASE 3 ] Carga a PostgreSQL...")
        records = load_to_postgres(df_prices, df_metrics)
        
        # REPORTE
        logger.info("[ FASE 4 ] Generando reporte...")
        report_file = generate_report()
        
        # LOG DE ÉXITO
        log_execution("success", records, f"Reporte: {report_file}")
        
        logger.info("=" * 50)
        logger.info(f"✅ PIPELINE COMPLETADO — {records} registros cargados")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ PIPELINE FALLIDO: {e}")
        log_execution("error", 0, str(e))
        raise


if __name__ == "__main__":
    run_pipeline()