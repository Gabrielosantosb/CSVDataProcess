import datetime
import threading
import time
import pandas as pd
import pprint
from io import StringIO



# ----------------------------------------------------------------------------
# Variaveis do programa
# ----------------------------------------------------------------------------

# Nome do arquivo que sera processado
file_name = ".\\data\\small_dataset.csv"

# Mensagens de saida
msg_output = []
thread_final_result = {}
# Guarda o resultado parcial de cada thread
threads_parcial_results = {
    "transacao_pais" : [],
    "media_preco_produto": [],
    "total_vendas_empresa": [],
    "quantidade_transacoes_pagamento": [],
    "distribuicao_vendas": [],
    "transacoes_comuns_cidade":[],
    "media_gastos_usuario": [],
    "total_vendas_moeda": []
}

# Quantidade de bytes de cada leitura
BUFSIZE = 16 * 1024 * 1024

# Controla quantidade de threads em exec
# Quantidade de threads sempre vai ser threads + 1  (O programa principal sempre é contado como uma thread)
LIMIT_OF_THREADS = 4  

# Iniciar variaveis
count = 0
start_time = datetime.datetime.now()
msg_output.append(f"Inicio Processamento - {start_time}")


# ----------------------------------------------------------------------------
# Funcoes de processamento de dados
# ----------------------------------------------------------------------------


def create_column_totalprice(pdf):
    pdf['total_price'] = pdf['quantity'] * pdf['price_per_unit']
    return pdf
    

def calcular_transacao_pais(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf_country = pdf.groupby(['country'])['country'].agg(['count'])  
    return pdf_country.to_dict()

def calcular_media_preco_produto(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf_product = pdf.groupby('product_description')['price_per_unit'].agg(['sum','count']) 
    return pdf_product.to_dict()


def calcular_vendas_por_empresa(dataBlock):
    
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")

    # Criando total price
    pdf = create_column_totalprice(pdf)
    pdf_empresa = pdf.groupby("company_id")["total_price"].agg(['sum'])
    return pdf_empresa.to_dict()

def calcular_transacao_por_pagamento(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf_payment_method = pdf.groupby('payment_method')['payment_method'].agg(['count'])
    return pdf_payment_method.to_dict()

def calcular_distribuicao_vendas(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    
    # Certifique-se de que 'transaction_date' seja do tipo datetime
    pdf['transaction_date'] = pd.to_datetime(pdf['transaction_date'])
    
    # Crie a coluna 'month_year' para representar o mês/ano da transação
    pdf['month_year'] = pdf['transaction_date'].dt.to_period('M')
    
    # Agrupe por 'month_year' e conte o número de transações para cada período
    pdf_distribuicao_vendas = pdf.groupby('month_year')['month_year'].agg(['count'])
    
    return pdf_distribuicao_vendas.to_dict()

def calcular_transacoes_por_cidade(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf_transacoes_cidade = pdf.groupby('city')['city'].agg(['count'])
    return pdf_transacoes_cidade.to_dict()

def calcular_media_gastos_usuario(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf = create_column_totalprice(pdf)
    pdf_media_gastos_usuario = pdf.groupby('user_id')['total_price'].agg(['mean'])
    return pdf_media_gastos_usuario.to_dict()

def calcular_total_vendas_moeda(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf = create_column_totalprice(pdf)
    pdf_total_vendas_moeda = pdf.groupby('currency')['total_price'].agg(['sum'])
    return pdf_total_vendas_moeda.to_dict()

def imprimir_resultados():
    # media_relatorio = int(input("Deseja ver o relatório resumido?\n[1]-Sim\n[2]-Não"))

    print("================ Transações por País================")
    if media_relatorio == 1:
        for pais, transacoes in list(thread_final_result['transacao_pais'].items())[:3]:
            print(f"{pais}: {transacoes}")
    else:
        for pais, transacoes in thread_final_result['transacao_pais'].items():
            print(f"{pais}: {transacoes}")

    print("================ Média de Preço por Produto===============")
    if media_relatorio == 1:
        for produto, dados in list(thread_final_result["media_preco_produto"].items())[:3]:
            avg = dados.get("avg", 0.0)
            print(f"{produto}: {avg:.2f}")
    else:
        for produto, dados in thread_final_result["media_preco_produto"].items():
            avg = dados.get("avg", 0.0)
            print(f"{produto}: {avg:.2f}")

    print("================ Total Vendas por Empresa ================")
    if media_relatorio == 1:
        for company_id, dados in list(thread_final_result["total_vendas_empresa"].items())[:3]:
            total_sales = dados.get("sum", 0.0)
            print(f"Empresa {company_id}: {total_sales:.2f}")
    else:
        for company_id, dados in thread_final_result["total_vendas_empresa"].items():
            total_sales = dados.get("sum", 0.0)
            print(f"Empresa {company_id}: {total_sales:.2f}")

    print("================ Quantidade de transações por método de pagamento================")
    if media_relatorio == 1:
        for pagamento, count in list(thread_final_result["quantidade_transacoes_pagamento"].items())[:3]:
            print(f"{pagamento}: {count}")
    else:
        for pagamento, count in thread_final_result["quantidade_transacoes_pagamento"].items():
            print(f"{pagamento}: {count}")

    print("================ Distribuição de vendas por mês/ano================")
    if media_relatorio == 1:
        for month_year, count in list(thread_final_result["distribuicao_vendas"].items())[:3]:
            print(f"{month_year}: {count}")
    else:
        for month_year, count in thread_final_result["distribuicao_vendas"].items():
            print(f"{month_year}: {count}")

    print("================ Transações mais comuns por cidade.================")
    if media_relatorio == 1:
        for city, count in list(thread_final_result["transacoes_comuns_cidade"].items())[:3]:
            print(f"{city}: {count}")
    else:
        for city, count in thread_final_result["transacoes_comuns_cidade"].items():
            print(f"{city}: {count}")

    print("================ Média de gastos por usuário.================")
    if media_relatorio == 1:
        for user_id, mean in list(thread_final_result["media_gastos_usuario"].items())[:3]:
            print(f"Usuário {user_id}: {mean['mean']:.2f}")
    else:
         for user_id, mean in list(thread_final_result["media_gastos_usuario"].items()):
            print(f"Usuário {user_id}: {mean['mean']:.2f}")

    print("================ Total de vendas em cada moeda.================")
    if media_relatorio == 1:
        for currency, data in list(thread_final_result["total_vendas_moeda"].items())[:3]:
            total_sales = data.get("sum", 0.0)  
            print(f"{currency}: {total_sales:.2f}")
    else:
        for currency, data in list(thread_final_result["total_vendas_moeda"].items()):
            total_sales = data.get("sum", 0.0)  
            print(f"{currency}: {total_sales:.2f}")
    print("===================================================================")

    
# Funcao de processamento de dados
def thread_processa_blocos(dataBlock, numBlock):
    # msg_output.append(f"Inicio processamento bloco de dados nº {numBlock} - {datetime.datetime.now()}")  
    threads_parcial_results["transacao_pais"].append(calcular_transacao_pais(dataBlock))
    threads_parcial_results["media_preco_produto"].append(calcular_media_preco_produto(dataBlock))
    threads_parcial_results["total_vendas_empresa"].append(calcular_vendas_por_empresa(dataBlock))
    threads_parcial_results["quantidade_transacoes_pagamento"].append(calcular_transacao_por_pagamento(dataBlock))
    threads_parcial_results["distribuicao_vendas"].append(calcular_distribuicao_vendas(dataBlock))
    threads_parcial_results["transacoes_comuns_cidade"].append(calcular_transacoes_por_cidade(dataBlock))
    threads_parcial_results["media_gastos_usuario"].append(calcular_media_gastos_usuario(dataBlock))
    threads_parcial_results["total_vendas_moeda"].append(calcular_total_vendas_moeda(dataBlock))
    # msg_output.append(f"Fim processamento bloco de dados nº {numBlock} - {datetime.datetime.now()}")



# ----------------------------------------------------------------------------
# Programa principal
# ----------------------------------------------------------------------------


# Ler o bloco do arquivo        
media_relatorio = int(input("Deseja ver o relatório resumido em 3 linhas?\n[1]-Sim\n[2]-Não\n"))
with open(file_name, "r") as infile:
    # Ler o cabecalho do arquivo 
    FILE_HEAD = infile.readline() 
    # Ler os blocos de dados do arquivo
    while True:
        lines = infile.readlines(BUFSIZE)
        if not lines:
            break
        text_block = FILE_HEAD + "\n" + "".join(lines)
        count += 1
        # msg_output.append(f"Leitura do bloco do arquivo {count} - {datetime.datetime.now() - start_time}")        
        
        # Dispara a thread para o bloco lida do arquivo
        
        thread_block = threading.Thread(target=thread_processa_blocos, args=(text_block, count,))
        thread_block.start()
        print(f"*** Executando nova thread - {threading.active_count() - 1} thread em execução\n")

        #Limita o valor das threads de acordo com o estabelecido
        while (threading.active_count() >= (LIMIT_OF_THREADS + 1)):
            print(f">>> Tem coisa acontecendo - {threading.active_count() - 1} thread em execução\n")
            time.sleep(1)  

# Aguarda o termino das threads de todos processamento de dados
while (threading.active_count() > 1):
    print(f">>> Tem coisa acontecendo - {threading.active_count() - 1} thread em execução\n")
    time.sleep(1)  

# Cálculo resultados finais(juntando os parciais)
def calcular_transacao_pais_final():
    msg_output.append(f"Totalização dos resultados parciais de transações por país - {datetime.datetime.now()}")  
    thread_final_result["transacao_pais"] = {}
    for transacao_pais in threads_parcial_results["transacao_pais"]:
        for count_key in transacao_pais["count"].keys():
            if not count_key in thread_final_result["transacao_pais"].keys():
                thread_final_result["transacao_pais"][count_key] = {}
                thread_final_result["transacao_pais"][count_key]["count"] = 0
            thread_final_result["transacao_pais"][count_key]["count"] += transacao_pais["count"][count_key]
            
    thread_final_result["media_preco_produto"] = {}

def calcular_media_preco_produto_final():
    msg_output.append(f"Totalização dos resultados parciais de média por produto - {datetime.datetime.now()}")  
    for media_preco in threads_parcial_results["media_preco_produto"]:

        # Calculo o total da sum e count para cada produto
        for count_key in media_preco["count"].keys():
            if not count_key in thread_final_result["media_preco_produto"].keys():
                thread_final_result["media_preco_produto"][count_key] = {}
                thread_final_result["media_preco_produto"][count_key]["count"] = 0
                thread_final_result["media_preco_produto"][count_key]["sum"] = 0
            thread_final_result["media_preco_produto"][count_key]["count"] += media_preco["count"][count_key]
            thread_final_result["media_preco_produto"][count_key]["sum"] += media_preco["sum"][count_key]
        
        # Calcula a media para cada produto
        for count_key in media_preco["count"].keys():
            thread_final_result["media_preco_produto"][count_key]["avg"] = thread_final_result["media_preco_produto"][count_key]["sum"]/thread_final_result["media_preco_produto"][count_key]["count"]

def calcular_total_vendas_empresa_final():
    msg_output.append(f"Totalização dos resultados parciais de vendas por empresa - {datetime.datetime.now()}")
    thread_final_result["total_vendas_empresa"] = {}

    for vendas_empresa in threads_parcial_results["total_vendas_empresa"]:
        # Calcula o total da soma para cada empresa
        for company_id, total_price in vendas_empresa["sum"].items():
            if not company_id in thread_final_result["total_vendas_empresa"].keys():
                thread_final_result["total_vendas_empresa"][company_id] = {}
                thread_final_result["total_vendas_empresa"][company_id]["sum"] = 0
            thread_final_result["total_vendas_empresa"][company_id]["sum"] += total_price

def calcular_transacao_por_pagamento_final():

    msg_output.append(f"Totalização dos resultados parciais de transações por método de pagamento - {datetime.datetime.now()}")
    thread_final_result["quantidade_transacoes_pagamento"] = {}

    for transacao_por_pagamento in threads_parcial_results["quantidade_transacoes_pagamento"]:
        # Calcula o total da contagem para cada método de pagamento
        for payment_method, count in transacao_por_pagamento["count"].items():
            if not payment_method in thread_final_result["quantidade_transacoes_pagamento"].keys():
                thread_final_result["quantidade_transacoes_pagamento"][payment_method] = {}
                thread_final_result["quantidade_transacoes_pagamento"][payment_method]["count"] = 0
            thread_final_result["quantidade_transacoes_pagamento"][payment_method]["count"] += count

def calcular_distribuicao_vendas_final():
    msg_output.append(f"Totalização dos resultados parciais de distribuição de vendas por mês/ano - {datetime.datetime.now()}")
    thread_final_result["distribuicao_vendas"] = {}

    for distribuicao_vendas in threads_parcial_results["distribuicao_vendas"]:
        # Calcula o total da contagem para cada mês/ano
        for month_year, count in distribuicao_vendas["count"].items():
            if not month_year in thread_final_result["distribuicao_vendas"].keys():
                thread_final_result["distribuicao_vendas"][month_year] = {}
                thread_final_result["distribuicao_vendas"][month_year]["count"] = 0
            thread_final_result["distribuicao_vendas"][month_year]["count"] += count

def transacoes_comuns_cidade_final():
    msg_output.append(f"Totalização dos resultados parciais de transações mais comuns por cidade - {datetime.datetime.now()}")
    thread_final_result["transacoes_comuns_cidade"] = {}

    for transacoes_cidade in threads_parcial_results["transacoes_comuns_cidade"]:
        # Calcula o total da contagem para cada cidade
        for city, count in transacoes_cidade["count"].items():
            if not city in thread_final_result["transacoes_comuns_cidade"].keys():
                thread_final_result["transacoes_comuns_cidade"][city] = {}
                thread_final_result["transacoes_comuns_cidade"][city]["count"] = 0
            thread_final_result["transacoes_comuns_cidade"][city]["count"] += count

def media_gastos_usuario_final():
    msg_output.append(f"Totalização dos resultados parciais de média de gastos por usuário - {datetime.datetime.now()}")
    thread_final_result["media_gastos_usuario"] = {}

    for media_gastos_usuario in threads_parcial_results["media_gastos_usuario"]:
        # Calcula o total da média para cada usuário
        for user_id, mean in media_gastos_usuario["mean"].items():
            if not user_id in thread_final_result["media_gastos_usuario"].keys():
                thread_final_result["media_gastos_usuario"][user_id] = {}
                thread_final_result["media_gastos_usuario"][user_id]["mean"] = 0.0
            thread_final_result["media_gastos_usuario"][user_id]["mean"] += mean

def calcular_total_vendas_moeda_final():
    msg_output.append(f"Totalização dos resultados parciais de total de vendas em cada moeda - {datetime.datetime.now()}")
    thread_final_result["total_vendas_moeda"] = {}

    for total_vendas_moeda in threads_parcial_results["total_vendas_moeda"]:
        # Calcula o total da soma para cada moeda
        for currency, total_price in total_vendas_moeda["sum"].items():
            if not currency in thread_final_result["total_vendas_moeda"].keys():
                thread_final_result["total_vendas_moeda"][currency] = {}
                thread_final_result["total_vendas_moeda"][currency]["sum"] = 0
            thread_final_result["total_vendas_moeda"][currency]["sum"] += total_price

# Chamando funções finais
calcular_transacao_pais_final()
calcular_media_preco_produto_final()
calcular_total_vendas_empresa_final()
calcular_transacao_por_pagamento_final()
calcular_distribuicao_vendas_final()
transacoes_comuns_cidade_final()
calcular_total_vendas_moeda_final()
media_gastos_usuario_final()

imprimir_resultados()
msg_output.append(f"Fim do processamento - Nº blocos processados {count} - Tempo total: {datetime.datetime.now() - start_time}")        

print("\n".join(msg_output))
print("\n\nAcabou")                