import os
import pandas as pd
from slugify import slugify

# Verifica se pasta existe
if not os.path.exists("./domestica"):
    print("Directory './domestica' does not exist.")
    exit(1)

# Garante a existência da pasta de saída
os.makedirs("./tratado", exist_ok=True)

# Lista todos os arquivos CSV
csv_files = []
for root, dirs, files in os.walk("./domestica"):
    for f in files:
        if f.endswith('.csv'):
            csv_files.append(os.path.join(root, f))

df_total = pd.DataFrame()

for csv_file in csv_files:
    try:
        df = pd.read_csv(csv_file, sep=';', encoding='latin1', skiprows=5)

        # Renomeia a primeira coluna
        df.rename(columns={df.columns[0]: "uf_estado"}, inplace=True)

        # Remove linhas de total
        fonte_idx = df[df["uf_estado"].astype(str).str.startswith("Total")].index
        if not fonte_idx.empty:
            df = df.loc[:fonte_idx[0]]
            df = df.iloc[:-1]

        # Remove a coluna "Total", se existir
        if "Total" in df.columns:
            df.drop(columns=["Total"], inplace=True)

        # Substitui '-' por 0 e converte para inteiro
        df.replace("-", 0, inplace=True)
        df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)

        # Extrai código e nome do estado
        df["cod_uf"] = df["uf_estado"].str.extract(r'^(\d+)').astype(int)
        df["estado"] = df["uf_estado"].str.replace(r'^\d+\s+', '', regex=True).str.strip()

        # Converte para formato longo
        df_melt = df.melt(id_vars=["estado", "cod_uf"], var_name="ano", value_name="casos")

        # Remove linhas inválidas de ano
        df_melt = df_melt[df_melt["ano"].str.fullmatch(r"\d{4}")]

        df_total = pd.concat([df_total, df_melt], ignore_index=True)

        print(f"✔ Processado: {csv_file}")

    except Exception as e:
        print(f"⚠ Erro ao processar {csv_file}: {e}")

# Agrupa e ordena
df_final = df_total.groupby(["estado", "cod_uf", "ano"], as_index=False)["casos"].sum()
df_final.sort_values(by=["cod_uf", "ano"], inplace=True)

# Salva
df_final.to_csv("./tratado/violencia_domestica_consolidada.csv", index=False, encoding="utf-8-sig")

print("\n✅ Consolidação Violencia Doméstica completa!")
print(df_final.head())


# Carrega arquivo, pulando as 6 primeiras linhas de texto
df = pd.read_csv("./suicidio/suicidios_2009-2023_UF_Original.csv", sep=';', encoding='latin1', skiprows=4)
df
# Renomeia a primeira coluna
df.rename(columns={df.columns[0]: "uf"}, inplace=True)

# Remove linhas que são regiões, totais, fontes ou que não contêm estados
fonte_idx = df[df["uf"].astype(str).str.startswith("Total")].index
if not fonte_idx.empty:
    df = df.loc[:fonte_idx[0]]
    df = df.iloc[:-1]
df = df[~df["uf"].str.contains("Região|Total|Fonte|Notas|Sistema|utilizados|Em ", na=False, case=False)]
# Extrai código numérico e nome do estado
# df["cod_uf"] = df["uf"].str.extract(r"(\d+)").astype(float)
df["estado"] = (
    df["uf"]
    .str.replace(r"^\.\.\s*", "", regex=True)  # remove os ".."
    .str.replace(r"^\d+\s*", "", regex=True)  # remove códigos
    .str.strip()
)
df
df = df.drop(columns=["uf"])

# Remove a coluna "Total" se existir
if "Total" in df.columns:
    df = df.drop(columns=["Total"])

if "2023" in df.columns:
    df = df.drop(columns=["2023"])
if "2022" in df.columns:
    df = df.drop(columns=["2022"])

# Converte para formato longo
df_long = df.melt(id_vars="estado", var_name="ano", value_name="suicidios")

# Limpa e converte tipos
df_long["ano"] = pd.to_numeric(df_long["ano"], errors="coerce")
df_long["suicidios"] = pd.to_numeric(df_long["suicidios"], errors="coerce").fillna(0).astype(int)

# Remove registros sem estado ou ano
df_long = df_long[df_long["estado"].notna()]
df_long = df_long[df_long["ano"].notna()]

# Ordena
df_long = df_long.sort_values(["estado", "ano"])

# Exporta
df_long.to_csv("./tratado/suicidios_tratado.csv", index=False, encoding="utf-8-sig")
print("✅ Arquivo tratado salvo como 'tratado/suicidios_tratado.csv'")


# Carrega os dois arquivos CSV
df_suicidios = pd.read_csv("./tratado/suicidios_tratado.csv")
df_violencia = pd.read_csv("./tratado/violencia_domestica_consolidada.csv")

df_violencia.rename(columns={"casos": "violencia_domestica"}, inplace=True)
# Faz a mesclagem (merge) com base nas colunas 'estado' e 'ano'
df_merged = pd.merge(df_suicidios, df_violencia, on=["estado", "ano"], how="outer")

pop_df = pd.read_csv("./ipeadata[27-05-2025-04-06]Pop-estimativa.csv", sep=";")
pop_df2010 = pd.read_csv("./ipeadata[29-05-2025-07-43]_populacao_2010.csv", sep=";")
# Adiciona a coluna de 2010 de pop_df2010 em pop_df
if "2010" in pop_df2010.columns:
    pop_df = pop_df.merge(pop_df2010[["Código", "2010"]], on="Código", how="left")
    
pop_df
pop_df.rename(columns={"Código": "cod_uf"}, inplace=True)
# pop_df.rename(columns={"Estado": "estado"}, inplace=True)
pop_df = pop_df.drop(columns=["Sigla", "Estado"])

colunas_ano = [col for col in pop_df.columns if col.strip().isdigit()]
pop_df = pop_df[["cod_uf"] + colunas_ano]
# Remover colunas irrelevantes e transformar em formato longo
pop_dflong = pop_df.melt(id_vars=["cod_uf"], var_name="ano", value_name="populacao")
pop_dflong["ano"] = pop_dflong["ano"].astype(int)


df_merged["ano"] = df_merged["ano"].astype(int)


df_merged = pd.merge(df_merged, pop_dflong, on=["cod_uf", "ano"], how="outer")

pib_df = pd.read_csv("./ipeadata[27-05-2025-10-21]_pib_estado.csv", sep=";")

pib_df.rename(columns={"Código": "cod_uf"}, inplace=True)
pib_df = pib_df.drop(columns=["Sigla", "Estado"])


colunas_ano = [col for col in pib_df.columns if col.strip().isdigit()]
pib_df = pib_df[["cod_uf"] + colunas_ano]
# Remover colunas irrelevantes e transformar em formato longo
pib_dflong = pib_df.melt(id_vars=["cod_uf"], var_name="ano", value_name="pib_estado")
pib_dflong["ano"] = pib_dflong["ano"].astype(int)

df_merged["ano"] = df_merged["ano"].astype(int)

df_merged = pd.merge(df_merged, pib_dflong, on=["cod_uf", "ano"], how="outer")


pop_df = pd.read_csv("./ipeadata[27-05-2025-10-30]_bolsa_familia_numero_benficiados.csv", sep=";")
pop_df.rename(columns={"Código": "cod_uf"}, inplace=True)
pop_df = pop_df.drop(columns=["Sigla", "Estado"])

colunas_ano = [col for col in pop_df.columns if col.strip().isdigit()]
pop_df = pop_df[["cod_uf"] + colunas_ano]
# Remover colunas irrelevantes e transformar em formato longo
pop_dflong = pop_df.melt(id_vars=["cod_uf"], var_name="ano", value_name="bolsa_familia_beneficiados")
pop_dflong["ano"] = pop_dflong["ano"].astype(int)

df_merged["ano"] = df_merged["ano"].astype(int)

df_merged = pd.merge(df_merged, pop_dflong, on=["cod_uf", "ano"], how="outer")


csv_files = []
for root, dirs, files in os.walk("./violencia"):
    for f in files:
        if f.endswith('.csv'):
            csv_files.append(os.path.join(root, f))


for csv_file in csv_files:
    try:
        df = pd.read_csv(csv_file, sep=';', encoding='latin1', skiprows=4)
        # Pega o nome do arquivo sem extensão e slugifica
        csv_basename = os.path.splitext(os.path.basename(csv_file))[0]
        name_slug = slugify(csv_basename)
        df_melt = pd.DataFrame()

        # Renomeia a primeira coluna
        df.rename(columns={df.columns[0]: "uf_estado"}, inplace=True)

        # Remove linhas de total
        fonte_idx = df[df["uf_estado"].astype(str).str.startswith("Total")].index
        if not fonte_idx.empty:
            df = df.loc[:fonte_idx[0]]
            df = df.iloc[:-1]

        # Remove a coluna "Total", se existir
        if "Total" in df.columns:
            df.drop(columns=["Total"], inplace=True)
        if "2023" in df.columns:
            df = df.drop(columns=["2023"])
        if "2022" in df.columns:
            df = df.drop(columns=["2022"])
        if "2024" in df.columns:
            df = df.drop(columns=["2024"])
    

        # Substitui '-' por 0 e converte para inteiro
        df.replace("-", 0, inplace=True)
        df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)

        # Extrai código e nome do estado
        df["cod_uf"] = df["uf_estado"].str.extract(r'^(\d+)').astype(int)
        df["estado"] = df["uf_estado"].str.replace(r'^\d+\s+', '', regex=True).str.strip()
        
        df.drop(columns=["uf_estado"], inplace=True)
        # df.drop(columns=["estado"], inplace=True)

        # Converte para formato longo
        df_melt = df.melt(id_vars=["estado", "cod_uf"], var_name="ano", value_name=name_slug)
        df_melt.drop(columns=["estado"], inplace=True)

        # Remove linhas inválidas de ano
        df_melt = df_melt[df_melt["ano"].str.fullmatch(r"\d{4}")]
        
        df_melt["ano"] = df_melt["ano"].astype(int)
        df_merged = pd.merge(df_merged, df_melt, on=["cod_uf", "ano"], how="outer")

        print(f"✔ Processado: {csv_file}")
        
    except Exception as e:
        print(f"⚠ Erro ao processar {csv_file}: {e}")

# Salva
# df_final.to_csv("./tratado/violencia_domestica_consolidada.csv", index=False, encoding="utf-8-sig")

print("\n✅ Consolidação Violencias")
# print(df_final.head())

# Ordena para facilitar visualização
df_merged = df_merged.sort_values(by=["estado", "ano"])
# Salva o resultado final
df_merged = df_merged[df_merged["ano"] != 2015]

output_path = "./tratado/dados_final.csv"
df_merged.to_csv(output_path, index=False, encoding="utf-8-sig", sep=";")

print(f"✅ Dados consolidados salvos em '{output_path}'")