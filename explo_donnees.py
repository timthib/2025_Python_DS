#Séries annuelles de plusieurs indicateurs : 
# 1- Nombre d'équivalent passager : mesure le total de ce qui est transporté : passagers + marchandises -> ne prend pas en compte la composition 
# 2- Nombre de passagers (pour mesurer l'évolution de la demande)
# 3- Nombre de vols (pour mesurer l'évolution de l'utilisation des aéroports, on pourra différencier selon les aéroports)

# Séries annuelles des Équivalents Passagers dans base LSN 
import pandas as pd
import glob
import os
    # 1. Trouver tous les fichiers
dossier_donnees_LSN = "data/traffic_aerien/asp-lsn-1990-2024/"
tous_les_fichiers_LSN = glob.glob(os.path.join(dossier_donnees_LSN, "ASP_LSN_*.csv"))    
liste_df = []
        
for nom_fichier in tous_les_fichiers_LSN:
    try:
        df_temp = pd.read_csv(nom_fichier, sep=';')
        liste_df.append(df_temp)
    except Exception as e:
        print(f" Erreur de chargement pour {nom_fichier} : {e}")
            
if not liste_df:
    print(f" Aucun fichier n'a pu être chargé dans le dossier .")
    print ('None')

    # 2. Concaténation (Fusion verticale)
df_concaténé_LSN = pd.concat(liste_df, ignore_index=True)
    
    # 3. Création de la colonne ANNEE à partir de 'ANMOIS' (format AAAAMM)
df_concaténé_LSN['ANNEE'] = df_concaténé_LSN['ANMOIS'].astype(str).str[:4].astype(int)
    
    # 4. Agrégation : Somme de la colonne Peq par ANNEE
df_annuel_peq_LSN = df_concaténé_LSN.groupby('ANNEE')['LSN_PEQ'].sum().reset_index()

df_annuel_peq_LSN


# Séries annuelles des Équivalents Passagers dans base CIE 
import pandas as pd
import glob
import os
    # 1. Trouver tous les fichiers
dossier_donnees_CIE = "data/traffic_aerien/asp-cie-2010-2024/"
tous_les_fichiers_CIE = glob.glob(os.path.join(dossier_donnees_CIE, "ASP_CIE_*.csv"))    
liste_df = []
        
for nom_fichier in tous_les_fichiers_CIE:
    try:
        df_temp = pd.read_csv(nom_fichier, sep=';')
        liste_df.append(df_temp)
    except Exception as e:
        print(f" Erreur de chargement pour {nom_fichier} : {e}")
            
if not liste_df:
    print(f" Aucun fichier n'a pu être chargé dans le dossier .")
    print ('None')

    # 2. Concaténation (Fusion verticale)
df_concaténé_CIE= pd.concat(liste_df, ignore_index=True)
    
    # 3. Création de la colonne ANNEE à partir de 'ANMOIS' (format AAAAMM)
df_concaténé_CIE['ANNEE'] = df_concaténé_CIE['ANMOIS'].astype(str).str[:4].astype(int)
    
    # 4. Agrégation : Somme de la colonne Peq par ANNEE
df_annuel_peq_CIE = df_concaténé_CIE.groupby('ANNEE')['CIE_PEQ'].sum().reset_index()
    
df_annuel_peq_CIE.head(5)


# On remarque que les deux séries ne sont pas les mêmes, 
# Le nombre annuel d'équivalent passagers est différent chaque année selon la base CIE (par compagnie) et LSN (par liaison)
