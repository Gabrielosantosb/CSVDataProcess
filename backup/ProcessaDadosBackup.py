import datetime
import threading
import time
import pandas as pd
from io import StringIO

# ----------------------------------------------------------------------------
# Funcoes de processamento de dados
# ----------------------------------------------------------------------------

# Funcao de processamento de dados
def thread_transacoes_pais(dataBlock, numBlock):
    dict_info = {}
    dict_info["start_msg"] = f"Inicio processamento bloco de dados nº {count} - {datetime.datetime.now()}"        
    
    data = StringIO(dataBlock)
    pdf = pd.read_csv(data, sep=",")

    pdf_country = pdf.groupby(['country'])['country'].count()
    threads_parcial_results.append(  pdf_country.values.tolist() )    
    
    dict_info["end_msg"] = f"Fim processamento bloco de dados nº {count} - {datetime.datetime.now()}"
    msg_output.append(dict_info)  

# ----------------------------------------------------------------------------
# Programa principal
# ----------------------------------------------------------------------------

# Nome do arquivo que sera processado
file_name = "C:\\git\\SO\\data\\middle_dataset.csv"

# Mensagens de saida
msg_output = []

# Guarda o resultado parcial de cada thread
threads_parcial_results = []

# Quantidade de bytes de cada leitura
BUFSIZE = 4 * 1024 * 1024

# Controla quantidade de threads em exec
# Quantidade de threads sempre vai ser threads + 1  (O programa principal sempre é contado como uma thread)
LIMIT_OF_THREADS = 4  

# Iniciar variaveis
count = 0
start_time = datetime.datetime.now()
msg_output.append(f"Inicio Processamento - {start_time}")

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
        
        thread_block = threading.Thread(target=thread_transacoes_pais, args=(text_block, count,))
        thread_block.start()
        
        print(f"*** Executando nova thread - {threading.active_count() - 1} thread em execução\n")

        while (threading.active_count() >= (LIMIT_OF_THREADS + 1)):
            print(f">>> Tem coisa acontecendo - {threading.active_count() - 1} thread em execução\n")
            time.sleep(1)  

msg_output.append(f"Fim do processamento - Nº blocos processados {count} - Tempo total: {datetime.datetime.now() - start_time}")        

print("\n".join(msg_output))
print("\n\nAcabou")                