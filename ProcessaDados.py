import datetime
import threading
import time
import pandas as pd
from io import StringIO



# ----------------------------------------------------------------------------
# Variaveis do programa
# ----------------------------------------------------------------------------

# Nome do arquivo que sera processado
file_name = ".\\data\\middle_dataset.csv"

# Mensagens de saida
msg_output = []

# Guarda o resultado parcial de cada thread
threads_parcial_results = {
    "transacao_pais" : [],
    "media_preco_produto": []
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


def sumarize_transacao_pais(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")

    pdf_country = pdf.groupby(['country'])['country'].count()
    list_data = pdf_country.to_dict()
    list_out = []
    for k in list_data.keys():
        list_out.append({"country": k, "count": list_data[k]})
    return list_out

def sumarize_media_preco_produto(dataBlock):
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")

    pdf_product = pdf.groupby('product_description')['price_per_unit'].agg(['mean', 'count'])
    list_data = pdf_product.to_dict()
    list_out = []
    for k in list_data["mean"].keys():
        list_out.append({"product": k, "mean": list_data["mean"][k], "count": list_data["count"][k]})
    return list_out

# Funcao de processamento de dados
def thread_processa_blocos(dataBlock, numBlock):
    msg_output.append(f"Inicio processamento bloco de dados nº {numBlock} - {datetime.datetime.now()}") 
    print("==========================================================")
    transacao_pais_result = sumarize_transacao_pais(dataBlock)
    media_preco_produto_result = sumarize_media_preco_produto(dataBlock)
    
    msg_output.append("Total de Transações por País:")
    for item in transacao_pais_result:
        msg_output.append(f"País {item['country']}: {item['count']} transações")

    msg_output.append("\nMédia de Preço por Produto:")
    for item in media_preco_produto_result:
        msg_output.append(f"Produto {item['product']}: Média de preço {item['mean']}")

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


msg_output.append(f"Fim do processamento - Nº blocos processados {count} - Tempo total: {datetime.datetime.now() - start_time}")        

print("\n".join(msg_output))
print("\n\nAcabou")                