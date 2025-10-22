from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

BASE_DIR = '/home/kleccio/Área de trabalho/portfolio' # Caminho base para encontrar a pasta do datalake
DATALAKE_PATH = f"{BASE_DIR}/datalake"
BRONZE_PATH = f"{DATALAKE_PATH}/bronze"
SILVER_PATH = f"{DATALAKE_PATH}/silver"
GOLD_PATH = f"{DATALAKE_PATH}/gold"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='CREATE_GOLD_TABLES',
    start_date=datetime(2025, 10, 12),
    schedule="0 3 * * *",
    catchup=True,
    default_args=default_args,
    tags=['gold']
) as dag:
     
    def create_gold_product_table(data_processamento: str) -> None:
        caminho_tabela_gold = f"{GOLD_PATH}/vendas/tb_gd_resumo_produtos_filiais"
        RECRIAR_TABELA = False
        
        try:
            df_resumo_produtos = DeltaTable.forPath(spark, caminho_tabela_gold)
            df_resumo_produtos.toDF().limit(1)

        except Exception as e:
            if 'DELTA_MISSING_DELTA_TABLE' in str(e):
                print('Tabela Delta não encontrada na camada prata. Realizando a criação de uma tabela nova.')
                RECRIAR_TABELA = True

            else:
                raise e

        if RECRIAR_TABELA:
            df_devolucao = spark.read.format('delta').load(f"{SILVER_PATH}/devolucao/devolucao")
            df_estoque = spark.read.format('delta').load(f"{SILVER_PATH}/estoque/estoque")
            df_vendas = spark.read.format('delta').load(f"{SILVER_PATH}/vendas/vendas")
        else:
            df_devolucao = spark.read.format('delta').load(f"{SILVER_PATH}/devolucao/devolucao").filter(f"data_devolucao = '{data_processamento}'")
            df_estoque = spark.read.format('delta').load(f"{SILVER_PATH}/estoque/estoque").filter(f"data_referencia = '{data_processamento}'")
            df_vendas = spark.read.format('delta').load(f"{SILVER_PATH}/vendas/vendas").filter(f"data_venda = '{data_processamento}'")


        # SUMARIZAÇÃO DEVOLUCAO
        df_devolucao_sumarizado = (
            df_devolucao
            .groupBy(
                F.col('data_devolucao').alias('CodigoData'),
                F.col('produto_id').alias('ProdutoId'),
                F.col('filial_id').alias('FilialId')
            )
            .agg(
                F.sum(F.col('quantidade')).alias('TotalDevolvido')
            )      
            .orderBy('CodigoData', 'ProdutoId','FilialId')
        )
        
        # SUMARIZAÇÃO ESTOQUE
        df_estoque_sumarizado = (
            df_estoque
            .groupBy(
                F.col('data_referencia').alias('CodigoData'),
                F.col('produto_id').alias('ProdutoId'),
                F.col('filial_id').alias('FilialId')
            )
            .agg(
                F.sum(F.col('quantidade_disponivel')).alias('TotalEmEstoque')
            )      
            .orderBy('CodigoData', 'ProdutoId','FilialId')
        )

        # SUMARIZAÇÃO VENDAS
        df_vendas_sumarizado = (
            df_vendas
            .groupBy(
                F.col('data_venda').alias('CodigoData'),
                F.col('produto_id').alias('ProdutoId'),
                F.col('filial_id').alias('FilialId')
            )
            .agg(
                F.sum(F.col('quantidade')).alias('TotalQuantidadeVendida'),
                F.round(
                    F.sum(F.col('valor_unitario'))
                , 2).alias('TotalValorVendido'),
                F.round(
                    F.sum(F.col('valor_unitario')) / F.sum(F.col('quantidade'))
                , 2).alias('TicketMedio')
            )      
            .orderBy('CodigoData', 'ProdutoId','FilialId')
        )

        df_resumo_produtos_new = (
            df_devolucao_sumarizado
            .join(df_estoque_sumarizado, ['CodigoData', 'ProdutoId', 'FilialId'], 'inner')
            .join(df_vendas_sumarizado, ['CodigoData', 'ProdutoId', 'FilialId'], 'inner')
            .orderBy('CodigoData', 'ProdutoId','FilialId')
        )

        df_resumo_produtos_new.show(5, False)
        
        if RECRIAR_TABELA:
            (
                df_resumo_produtos_new
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('overwrite')
                .save(caminho_tabela_gold)
            )
        else:
            df_resumo_produtos.delete(f"CodigoData = '{data_processamento}'")

            (
                df_resumo_produtos_new
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('append')
                .save(caminho_tabela_gold)
            )

    def create_gold_freight_table(data_processamento: str) -> None:
        caminho_tabela_gold = f"{GOLD_PATH}/logistica/tb_gd_resumo_fretes_pagos"
        RECRIAR_TABELA = False

        try:
            df_resumo_fretes = DeltaTable.forPath(spark, caminho_tabela_gold)
            df_resumo_fretes.toDF().limit(1)

        except Exception as e:
            if 'DELTA_MISSING_DELTA_TABLE' in str(e):
                print('Tabela Delta não encontrada na camada prata. Realizando a criação de uma tabela nova.')
                RECRIAR_TABELA = True

            else:
                raise e

        if RECRIAR_TABELA:
            df_fretes = spark.read.format('delta').load(f"{SILVER_PATH}/frete/frete")
        else:
            df_fretes = spark.read.format('delta').load(f"{SILVER_PATH}/frete/frete").filter(f"data_envio = '{data_processamento}'")

        df_fretes.createOrReplaceTempView('fretes')

        df_fretes_sumarizado = spark.sql(
            """
            SELECT 
                DATA_ENVIO AS CodigoData,
                FILIAL_ID as FilialId,
                transportadora as Transportadora,
                ROUND(SUM(VALOR_FRETE), 4) AS TotalFretePago
            FROM 
                fretes
            GROUP BY 
                DATA_ENVIO,
                FILIAL_ID,
                transportadora
            ORDER BY 
                DATA_ENVIO, 
                FILIAL_ID, 
                TRANSPORTADORA
            """
        )

        print('Escrevendo tabela na camada gold em: ', caminho_tabela_gold)
        if RECRIAR_TABELA:
            (
                df_fretes_sumarizado
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('overwrite')
                .save(caminho_tabela_gold)
            )
        else:
            df_resumo_fretes.delete(f"CodigoData = '{data_processamento}'")

            (
                df_fretes_sumarizado
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('append')
                .save(caminho_tabela_gold)
            )

    def create_gold_taxes_table(data_processamento: str) -> None:
        caminho_tabela_gold = f"{GOLD_PATH}/tributario/tb_gd_resumo_impostos_pagos"
        RECRIAR_TABELA = False

        try:
            df_resumo_impostos = DeltaTable.forPath(spark, caminho_tabela_gold)
            df_resumo_impostos.toDF().limit(1)

        except Exception as e:
            if 'DELTA_MISSING_DELTA_TABLE' in str(e):
                print('Tabela Delta não encontrada na camada prata. Realizando a criação de uma tabela nova.')
                RECRIAR_TABELA = True

            else:
                raise e

        if RECRIAR_TABELA:
            df_tributos = spark.read.format('delta').load(f"{SILVER_PATH}/tributos/tributos")
        else:
            df_tributos = spark.read.format('delta').load(f"{SILVER_PATH}/tributos/tributos").filter(f"data_pagamento = '{data_processamento}'")

        df_tributos_sumarizado = (
            df_tributos
            .groupBy(
                F.col('data_pagamento').alias('CodigoData'),
                F.col('filial_id').alias('FilialId'),
                F.col('tipo_imposto').alias('TipoImposto'),
            )
            .agg(
                F.round(
                    F.sum(F.col('valor_pago'))
                , 4).alias('TotalImpostoPago')
            )
            .orderBy('CodigoData', 
                    'FilialId', 
                    'TipoImposto', 
                    )
        )

        print('Escrevendo tabela na camada gold em: ', caminho_tabela_gold)
        if RECRIAR_TABELA:
            (
                df_tributos_sumarizado
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('overwrite')
                .save(caminho_tabela_gold)
            )
        else:
            df_resumo_impostos.delete(f"CodigoData = '{data_processamento}'")

            (
                df_tributos_sumarizado
                .write
                .format('delta')
                .option('mergeSchema', 'true')
                .mode('append')
                .save(caminho_tabela_gold)
            )

    # Sensores para aguardar as tasks da DAG Silver
    wait_vendas = ExternalTaskSensor(
        task_id='wait_ingest_vendas',
        external_dag_id='INGEST_BRONZE_TO_SILVER',
        external_task_id='ingest_vendas_bronze_to_silver',
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    wait_estoque = ExternalTaskSensor(
        task_id='wait_ingest_estoque',
        external_dag_id='INGEST_BRONZE_TO_SILVER',
        external_task_id='ingest_estoque_bronze_to_silver',
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    wait_devolucao = ExternalTaskSensor(
        task_id='wait_ingest_devolucao',
        external_dag_id='INGEST_BRONZE_TO_SILVER',
        external_task_id='ingest_devolucao_bronze_to_silver',
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    wait_frete = ExternalTaskSensor(
        task_id='wait_ingest_frete',
        external_dag_id='INGEST_BRONZE_TO_SILVER',
        external_task_id='ingest_frete_bronze_to_silver',
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    wait_tributos = ExternalTaskSensor(
        task_id='wait_ingest_tributos',
        external_dag_id='INGEST_BRONZE_TO_SILVER',
        external_task_id='ingest_tributos_bronze_to_silver',
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    # Tasks da camada gold
    create_gold_products = PythonOperator(
        task_id='create_gold_product_table',
        python_callable=create_gold_product_table
    )

    create_gold_freight = PythonOperator(
        task_id='create_gold_freight_table',
        python_callable=create_gold_freight_table
    )

    create_gold_taxes = PythonOperator(
        task_id='create_gold_taxes_table',
        python_callable=create_gold_taxes_table
    )

    # Dependências (aguarda sensores)
    [wait_vendas, wait_estoque, wait_devolucao] >> create_gold_products
    wait_frete >> create_gold_freight
    wait_tributos >> create_gold_taxes