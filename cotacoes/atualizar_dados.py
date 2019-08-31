
##################################################################################
### Main
##################################################################################
def main():
    
    import time
    use_paralellism = True
    if use_paralellism:
        import multiprocessing
        import psutil
    
    start_time = time.time()
    print("\n*** Processo iniciado!")
    
    import cotacoes
    
#    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['IBOV.sa'])
#    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['IBRA.sa'])
#    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['FII.sa'])
    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['IBRA.sa','FII.sa'])
#    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['ALL.sa','FII.sa'])
#    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['ALL.sa'])
#    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['BOVA11.sa'])
#    codigos_da_carteira_b3_acoes_eod = cotacoes.carregar_codigos(['ALL.sa','FII.sa', 'BOVA11.sa', 'IBRA.sa', 'IBOV.sa'])

    if use_paralellism:
        cpu_count = psutil.cpu_count(logical=False)
        print("\n*** Processo utilizando paralelismo com", cpu_count, "processos!")
        lista_codigos = [codigos_da_carteira_b3_acoes_eod[i::cpu_count] for i in range(cpu_count)]
        processes = []
        for i in range(cpu_count):
            processes.append(multiprocessing.Process(target = atualizar_cotacoes_b3_acoes_eod, args = (lista_codigos[i],)))
            processes[-1].start()
        for i_process in processes:
            i_process.join()
    else:
        atualizar_cotacoes_b3_acoes_eod(codigos_da_carteira_b3_acoes_eod)
    
    elapsed_time = time.time() - start_time    
    print("\n*** Processo concluído em %d minutos e %f segundos!" %(elapsed_time/60, elapsed_time%60))

    return

##################################################################################
### atualizar_cotacoes_b3_acoes_eod
##################################################################################
def atualizar_cotacoes_b3_acoes_eod(codigos_da_carteira):

    import pandas as pd
    from datetime import datetime as dt
    import datetime
    import os
    
    # As cotações de sérios historicas da bovespa não são ajustadas. Logo, os valores
    # passados não são alterados com o passar do tempo, e assim a atualização pode ser feito de
    # forma incremental, a partir da última data disponível na base de dados
    
    for codigo in codigos_da_carteira:
        codigo = codigo.replace('.sa','')
        filename_csv = 'dados/b3/cotacoes_diarias/'+codigo+'.sa.csv'
        print("[",codigo,"] Buscando cotações...")
        if os.path.isfile(filename_csv):
            df = pd.read_csv(filename_csv)
            if len(df['date'])>0:
                start_date = max(pd.to_datetime(df['date'],format='%d/%m/%Y')) + datetime.timedelta(days=1)
                n = len(df.index)
                min_date = min(pd.to_datetime(df['date'],format='%d/%m/%Y'))
                max_date = max(pd.to_datetime(df['date'],format='%d/%m/%Y'))
                print("    [",codigo,"] Partindo de cotações da base de dados de",min_date.strftime("%d/%m/%Y"),"a",max_date.strftime("%d/%m/%Y"))
            else:
                start_date = dt.strptime('01/01/1986','%d/%m/%Y')
                df = pd.DataFrame(columns=('date','open','high','low','close','neg','vol'))
                n = 0
                print("    [",codigo,"] Não há dados prévios na base")
#            return
        else:
            start_date = dt.strptime('01/01/1986','%d/%m/%Y')
            df = pd.DataFrame(columns=('date','open','high','low','close','neg','vol'))
            n = 0
            print("    [",codigo,"] Não há dados prévios na base")
        for year in range(max(1986,start_date.year),dt.today().year+1):
            print("    [",codigo,"] Processando dados do ano", year)
            file = open("fontes/b3/series_historicas/COTAHIST_A"+str(year)+".TXT", "r")
            next(file) # descarta primeira linha
            for line in file:
                line_stock_symbol = line.strip()[12:23].replace(' ','')
                if line_stock_symbol==codigo:
                    line_date = dt(int(line.strip()[2:6]), int(line.strip()[6:8]), int(line.strip()[8:10]))
#                    print(line_date)
                    if line_date >= start_date:
#                        print(line.strip())
                        line_open  = float(line.strip()[56:69].replace(' ',''))/100.0
                        line_high  = float(line.strip()[69:82].replace(' ',''))/100.0
                        line_low   = float(line.strip()[82:95].replace(' ',''))/100.0
                        line_close = float(line.strip()[108:121].replace(' ',''))/100.0
                        line_neg = int(line.strip()[147:152].replace(' ',''))
                        line_vol = int(line.strip()[170:188].replace(' ',''))
                        df.loc[n] = [line_date.strftime("%d/%m/%Y"), line_open, line_high, line_low, line_close, line_neg, line_vol]
                        n += 1
                        if n==1:
                            min_date = line_date
                            max_date = line_date
                        else:
                            min_date = min(line_date,min_date)
                            max_date = max(line_date,max_date)
            file.close()            
#        print(df.to_string())
        df.to_csv (filename_csv, index = None, header=True)
        if len(df.index) > 0:
            print("[",codigo,"] Foram extraídas cotações de",min_date.strftime("%d/%m/%Y"),"a",max_date.strftime("%d/%m/%Y"))
        else:
            print("[",codigo,"] Não há cotações disponíveis")

    return

##################################################################################
### Chamada de main()
##################################################################################
if __name__== "__main__":
    main()