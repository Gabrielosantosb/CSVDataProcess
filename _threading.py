# import threading
# import time
# import random



# -----IGNORA ESSE ARQUIVO
# def thread_function(name):
#     tempo = random.randint(2, 5)
#     print(f"Inicio da thread {name} - Esperando {tempo}s\n")
#     time.sleep(tempo)
#     print(f"Fim da thread {name}\n")

# for i in range(1, 5 + 1):        
#     thread_block = threading.Thread(target=thread_function, args=(i,))
#     thread_block.start()

# while (threading.active_count() > 1):
#     print(f"Tem coisa acontecendo - {threading.active_count() - 1} thread em execução\n")
#     time.sleep(1)    

# print(f"Programa acaba aqui - {threading.active_count()} thread em execução\n")