import pandas as pd
import gc
pd.options.display.float_format = "{:.2f}".format
base_csv = 'base/vendas.csv'

def maoior_produto_canal(base_csv=base_csv):
    # selecionado apenas as colunas a serem utilizadas no DF, garantindo performance na memoria
    colunas = ['Item Type', 'Sales Channel', 'Units Sold',]
    # classificao do tipo do dado, garantindo performance na memoria - dado do tipo OBJECT ocupam MAIS ESPAÇO NA MEMORIA
    tipos = {'Region': 'category', 'Country': 'category', 'Item Type': 'category', 'Sales Channel': 'category', 'Units Sold': 'int16'}
    
    maior_produto_canal = pd.DataFrame()
    
    for chunk in pd.read_csv(base_csv, dtype=tipos, usecols=colunas, chunksize=600):
        #para cada interacao com o chunk agrupa e soma os valores para adicionar em um DF final
        chunk_grouped = chunk.groupby(['Item Type', 'Sales Channel'])['Units Sold'].sum().reset_index()
        maior_produto_canal = pd.concat([maior_produto_canal, chunk_grouped]).groupby(['Item Type', 'Sales Channel'])['Units Sold'].sum().reset_index()

        # remove a variavel, limpando a memoria
        del chunk_grouped
    # retorna um DF com o produto que teve a maior quantidade no canal
    return maior_produto_canal.loc[maior_produto_canal.groupby('Sales Channel')['Units Sold'].idxmax()].sort_values(by='Sales Channel')

def venda_pais_regiao(base_csv=base_csv):
    # selecionado apenas as colunas a serem utilizadas no DF, garantindo performance na memoria
    colunas = ['Region', 'Country', 'Total Revenue']
    # classificao do tipo do dado, garantindo performance na memoria - dado do tipo OBJECT ocupam MAIS ESPAÇO NA MEMORIA
    tipos = {'Region': 'category', 'Country': 'category'}
    
    venda_pais_regiao = pd.DataFrame()
    
    for chunk in pd.read_csv(base_csv, dtype=tipos, usecols=colunas, chunksize=600):
        chunk_grouped = chunk.groupby(['Country', 'Region'])['Total Revenue'].sum().reset_index()
        venda_pais_regiao = pd.concat([venda_pais_regiao, chunk_grouped]).groupby(['Country', 'Region'])['Total Revenue'].sum().reset_index()

        # remove a variavel, limpando a memoria
        del chunk_grouped

    # retorna um DF com a maior receita, agrupado por pais e regiao 
    return venda_pais_regiao.loc[venda_pais_regiao.groupby('Region')['Total Revenue'].idxmax()].sort_values(by='Total Revenue', ascending=False)

def media_produto_anomes(base_csv=base_csv):
    # selecionado apenas as colunas a serem utilizadas no DF, garantindo performance na memoria
    colunas = ['Item Type', 'Order Date', 'Total Revenue']
    # classificao do tipo do dado, garantindo performance na memoria - dado do tipo OBJECT ocupam MAIS ESPAÇO NA MEMORIA
    tipos = {'Item Type': 'category'}
    # garante que a coluna esteja com o tipo DATE, possibilitando a interação com o dado, sem isso pode ocorrer erro na execucao
    dates= ['Order Date']

    media_produto_anomes = pd.DataFrame()
    
    for chunk in pd.read_csv(base_csv, dtype=tipos, usecols=colunas, parse_dates=dates, chunksize=600):
        # para cada chunk realiza o agrupamento e soma, na sequencia concatena em um novo DF agrupando e somando os valores
        chunk_grouped = chunk.groupby(['Item Type', 'Order Date'])['Total Revenue'].sum().reset_index()
        media_produto_anomes = pd.concat([media_produto_anomes, chunk_grouped]).groupby(['Item Type', 'Order Date'])['Total Revenue'].sum().reset_index()

        # remove a variavel, limpando a memoria
        del chunk_grouped

    # cria uma coluna para realizar agrupamento por ANO MES - será utilizada para a média dos valores
    media_produto_anomes['YearMonth'] = media_produto_anomes['Order Date'].dt.to_period('M')
    
    # remove a coluna Order Date, não será mais utilizada, desta forma ganho espaço na memoria
    media_produto_anomes.drop('Order Date', axis=1, inplace=True)
    
    # realiza uma soma da receita, agrupando os dados pelo produto e ano_mes
    media_produto_anomes.groupby(['Item Type', 'YearMonth'])['Total Revenue'].sum().reset_index()

    # faz a media agrupado pelo produto e ano mes
    media_produto_anomes = media_produto_anomes.groupby(['Item Type', 'YearMonth'])['Total Revenue'].mean().reset_index()

    # retorna um DF pivoteado, apresentando todos os produtos com suas respectivas receitas ao longo do periodo
    media_produto_anomes = media_produto_anomes.pivot(index='Item Type', columns='YearMonth', values='Total Revenue')

    return media_produto_anomes




# retorna o produto com maior quantidade do canal
print(maoior_produto_canal())

# retorna DF com a maior receita por Pais e Regiao
print(venda_pais_regiao())

# retorna DF pivoteado por ANO_MES com a media da receita no periodo
print(media_produto_anomes())
