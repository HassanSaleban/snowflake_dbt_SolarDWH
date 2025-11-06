#  Solar Data Warehouse — Cloud ELT Project 
![ELT-solar](https://github.com/user-attachments/assets/69ff73d7-8de2-46fb-b99f-137f2270163e)

## Description du projet

Le projet **Solar Data Warehouse** a pour objectif de construire une **architecture ELT moderne dans le cloud** pour analyser les **flux de production et de commerce des technologies solaires** (photovoltaïques, batteries, etc.) entre la **Chine**, l’**Union Européenne**, et d’autres marchés internationaux.

Ce pipeline de données intègre plusieurs technologies cloud :
- **AWS S3** pour le stockage brut (Data Lake)
- **Snowflake** pour l’entreposage et le calcul analytique
- **dbt Cloud** pour la transformation, la modélisation et le versioning GitHub
- **Python (BeautifulSoup, Selenium, Pandas)** pour l’extraction et la préparation des données

---

##  Architecture Cloud du Projet

### 🔹 Vue d’ensemble

![Architecture ELT](./ELT-solar.jpg)

1. **Extraction (E)**  
   Les données sont extraites via des scripts Python utilisant **Selenium** et **BeautifulSoup** depuis des sources ouvertes (UN Comtrade, CleanTech, etc.).

2. **Chargement (L)**  
   Les fichiers `.csv` nettoyés sont déposés dans un **bucket AWS S3** :  
   `s3://solar-s3-staging/`

3. **Transformation (T)**  
   Les données sont chargées dans **Snowflake** (schéma `BRONZE`), puis transformées dans **dbt Cloud** via plusieurs couches :
   - Bronze → données brutes
   - Staging (Silver) → nettoyage et mapping
   - Marts (Gold) → tables analytiques finales (facts et dimensions)

4. **Versioning**  
   Tout le code dbt est versionné sur **GitHub** pour assurer la traçabilité et la reproductibilité des transformations.

---

##  Architecture Snowflake + dbt

![Snowflake Gold Layer](./Goldsnowflake.png) <img width="1874" height="941" alt="Goldsnowflake" src="https://github.com/user-attachments/assets/65447711-0c64-4de7-a6af-30a8a1648804" />


###  Couches de données
- **BRONZE** : ingestion brute depuis S3 (`raw_*`)
- **STAGING** : préparation et harmonisation (`stg_*`)
- **MARTS** : modèles analytiques (`dim_*`, `fact_*`)

###  Tables principales
| Schéma | Table | Type | Description |
|--------|--------|------|-------------|
| MARTS | DIM_DATE | Dimension | Table de dates |
| MARTS | DIM_COUNTRY | Dimension | Référentiel pays |
| MARTS | DIM_PRODUCT | Dimension | Référentiel produits |
| MARTS | FACT_SOLAR_PRODUCTION | Fait | Production solaire |
| MARTS | FACT_TRADE | Fait | Flux commerciaux UE/Chine |

---

##  Modélisation dbt

![dbt Lineage Graph](./datalineage.png) <img width="1871" height="944" alt="datalineage" src="https://github.com/user-attachments/assets/d429d1de-de30-4694-aa31-546e9e2e3569" />


Le graphe montre le flux complet :
- Sources : `bronze.raw_*`
- Staging : `stg_*`
- Dimensions : `dim_*`
- Faits : `fact_*`

### 🔹 Exemples
- `stg_china_exports.sql` → nettoie les exportations chinoises  
- `stg_eu_imports.sql` → harmonise les importations de l’UE  
- `fact_trade.sql` → combine les flux import/export

---

##  Stockage AWS S3

![S3 Bucket](./storageS3.png)<img width="1854" height="837" alt="storageS3" src="https://github.com/user-attachments/assets/b0d1012e-8d04-492a-bb4c-7637f1e5f8e3" />


Les fichiers bruts sont stockés dans :
`s3://solar-s3-staging/`

Exemples :
- `batteries.csv`
- `photovoltaïque.csv`
- `productions_ue.csv`
- `union_europeen.csv`

---

## ⚙️ Pipeline de transformation

![dbt Execution](./Screenshot%202025-11-06%20134421.png)<img width="1867" height="921" alt="Screenshot 2025-11-06 134421" src="https://github.com/user-attachments/assets/1732a861-78fe-4ad0-9524-443c9ec7a906" />


Résultat de l’exécution :
-  9 modèles exécutés avec succès
-  Temps total ≈ 12 secondes
-  Déploiement dans `Snowflake` via `dbt Cloud`

---

## 🔍 Vérification dans Snowflake

![Snowflake Query](./ab52c1ee-13ea-4a04-b6d4-3d027ee5c59a.png)

```sql
SELECT * FROM SOLARDB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'MARTS';
```

---

##  Technologies utilisées

| Domaine | Technologie | Description |
|----------|-------------|-------------|
| Extraction | Python (Selenium, BeautifulSoup) | Scraping et collecte |
| Stockage | AWS S3 | Data Lake |
| Transformation | dbt Cloud | Modélisation SQL |
| Entrepôt | Snowflake | Data Warehouse |
| Versioning | GitHub | CI/CD |
| Documentation | dbt Docs | Lineage & doc interactive |

---

## 👥 Auteur
**Hassan Saleban**  
📍 *Bruxelles Formation — Data Engineer *  
[GitHub : HassanSaleban](https://github.com/HassanSaleban)


