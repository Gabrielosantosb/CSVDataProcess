import datetime
import threading
import time
import pandas as pd
from io import StringIO



# ----------------------------------------------------------------------------
# Variaveis do programa
# ----------------------------------------------------------------------------

# Nome do arquivo que sera processado
file_name = "C:\\Git\\Meus projetos\\CSVDataProcess\\data\\middle_dataset.csv"

# Mensagens de saida
msg_output = []
thread_final_result = {}
# Guarda o resultado parcial de cada thread
threads_parcial_results = {
    "transacao_pais" : [],
    "media_preco_produto": [],
    "total_vendas_empresa": []
}

# Quantidade de bytes de cada leitura
BUFSIZE = 4 * 1024 * 1024

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
    

def sumarize_transacao_pais(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf_country = pdf.groupby(['country'])['country'].agg(['count'])  
    return pdf_country.to_dict()

def sumarize_media_preco_produto(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")
    pdf_product = pdf.groupby('product_description')['price_per_unit'].agg(['sum','count']) 
    return pdf_product.to_dict()


def sumarize_vendas_por_empresa(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")

    # Criando total price
    pdf = create_column_totalprice(pdf)
    pdf_empresa = pdf.groupby("company_id")["total_price"].agg(['sum'])
    return pdf_empresa.to_dict()

# Funcao de processamento de dados
def thread_processa_blocos(dataBlock, numBlock):
    msg_output.append(f"Inicio processamento bloco de dados nº {numBlock} - {datetime.datetime.now()}")  
    threads_parcial_results["transacao_pais"].append(sumarize_transacao_pais(dataBlock))
    threads_parcial_results["media_preco_produto"].append(sumarize_media_preco_produto(dataBlock))
    threads_parcial_results["total_vendas_empresa"].append(sumarize_vendas_por_empresa(dataBlock))
    msg_output.append(f"Fim processamento bloco de dados nº {numBlock} - {datetime.datetime.now()}")



# ----------------------------------------------------------------------------
# Programa principal
# ----------------------------------------------------------------------------


# Ler o bloco do arquivo        
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
        msg_output.append(f"Leitura do bloco do arquivo {count} - {datetime.datetime.now() - start_time}")        
        
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
# threads_parcial_results["transacao_pais"][0]["count"]["Algeria"]
msg_output.append(f"Totalização dos resultados parciais de transações por país - {datetime.datetime.now()}")  
thread_final_result["transacao_pais"] = {}
for tp in threads_parcial_results["transacao_pais"]:
    for count_key in tp["count"].keys():
        if not count_key in thread_final_result["transacao_pais"].keys():
            thread_final_result["transacao_pais"][count_key] = {}
            thread_final_result["transacao_pais"][count_key]["count"] = 0
        thread_final_result["transacao_pais"][count_key]["count"] += tp["count"][count_key]

msg_output.append(f"Totalização dos resultados parciais de média por produto - {datetime.datetime.now()}")  
thread_final_result["media_preco_produto"] = {}
for tp in threads_parcial_results["media_preco_produto"]:
    # Calculo o total da sum e count para cada produto
    for count_key in tp["count"].keys():
        if not count_key in thread_final_result["media_preco_produto"].keys():
            thread_final_result["media_preco_produto"][count_key] = {}
            thread_final_result["media_preco_produto"][count_key]["count"] = 0
            thread_final_result["media_preco_produto"][count_key]["sum"] = 0
        thread_final_result["media_preco_produto"][count_key]["count"] += tp["count"][count_key]
        thread_final_result["media_preco_produto"][count_key]["sum"] += tp["sum"][count_key]
    # Calcula a media para cada produto
    for count_key in tp["count"].keys():
        thread_final_result["media_preco_produto"][count_key]["avg"] = thread_final_result["media_preco_produto"][count_key]["sum"]/thread_final_result["media_preco_produto"][count_key]["count"]

msg_output.append(f"Fim do processamento - Nº blocos processados {count} - Tempo total: {datetime.datetime.now() - start_time}")        

print("\n".join(msg_output))
print("\n\nAcabouu")                