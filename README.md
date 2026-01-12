<div align="center">

# SUNAT Parser & Financial Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-150458)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791)
![License](https://img.shields.io/badge/License-Proprietary-red)

**[ 🇺🇸 English ](#-english-version) | [ 🇵🇪 Español ](#-versión-en-español)**

</div>

---

<a name="-english-version"></a>
## 🇺🇸 English Version

### 📌 Project Overview

This project is a specialized **ETL (Extract, Transform, Load) solution** designed to bridge the gap between raw government tax data and financial analytics. It automates the processing of massive volumes of Electronic Payment Vouchers (CPE) and Electronic Books (SIRE/PLE) issued by **SUNAT (Perú)**.

Built with a focus on **Data Engineering** and **Financial Compliance**, this tool parses complex XML (UBL 2.1), ZIP, and flat text files, transforming them into structured relational data (SQL) or analytical datasets (CSV) for auditing and ERP integration.

### 🚀 Key Engineering Features

*   **High-Throughput Parsing**: Efficiently handles large datasets using `lxml` for XML parsing and `pandas` for vectorized data manipulation.
*   **Strategy Design Pattern**: Implements a modular architecture where each document type (Invoice, Credit Note, Payroll) has its own processor strategy, adhering to the **Open/Closed Principle**.
*   **Data Normalization**: Converts heterogeneous government formats into a unified schema, handling currency conversion, tax breakdowns (IGV, ISC), and unique ID generation (CUI) for accounting traceability.
*   **Dual Output Modes**:
    *   **Audit Mode**: Generates flat CSV files for quick Excel-based reconciliation.
    *   **Integration Mode**: Direct injection into **PostgreSQL** using `SQLAlchemy` ORM.

### 🛠️ Tech Stack

*   **Language**: Python 3.10+ (Type Hinting, Dataclasses).
*   **Core Libraries**: `Pandas` (Data Transformation), `lxml` (XML Parsing), `NumPy`.
*   **Database**: `SQLAlchemy` (ORM), `psycopg2` (PostgreSQL Driver).
*   **Configuration**: `python-dotenv` for environment security.

### 💻 Quick Start

1.  **Setup Environment**:
    ```bash
    git clone <repo-url>
    python -m venv venv
    source venv/bin/activate  # or .\venv\Scripts\activate on Windows
    pip install -r requirements.txt
    ```

2.  **Run Pipeline (CSV Output)**:
    ```bash
    python src/main.py "D:/path/to/sunat/files" --output_format csv
    ```

---

<a name="-versión-en-español"></a>
## 🇵🇪 Versión en Español

### 📌 Descripción del Proyecto

Este proyecto es una herramienta de **Ingeniería de Datos aplicada al ámbito Contable y Financiero**. Su objetivo es automatizar la conciliación tributaria procesando masivamente los archivos XML y reportes de la **SUNAT**.

Diseñado por un especialista con perfil híbrido (Economía + Desarrollo), el sistema entiende la lógica contable peruana, permitiendo transformar la data cruda de la clave SOL en información financiera lista para análisis o auditoría.

### 💼 Funcionalidades de Negocio

*   **Procesamiento Multi-Formato**:
    *   **Facturación Electrónica (UBL 2.1)**: Facturas, Boletas, Notas de Crédito/Débito, Guías de Remisión.
    *   **SIRE (Sistema Integrado de Registros Electrónicos)**: Procesa las propuestas RVIE (Ventas) y RCE (Compras) desde los ZIPs descargados de SUNAT.
    *   **Planillas (T-Registro)**: Extrae y consolida reportes laborales (TRA, IDE, SSA).
*   **Inteligencia Contable**:
    *   Generación de **CUI (Código Único de Identificación)** para cruzar información entre Libros y Comprobantes.
    *   Validación de duplicados y consistencia de datos.
    *   Desglose automático de bases imponibles, inafectos, exonerados e impuestos (IGV, ICBPER).

### ⚙️ Guía de Ejecución

El sistema funciona mediante línea de comandos (CLI), ideal para automatizar tareas recurrentes de cierre mensual.

#### 1. Modo Auditoría (CSV)
Ideal para revisiones rápidas en Excel sin configurar bases de datos.
```bash
python src/main.py "D:/mis_facturas_xml" --output_format csv
```

#### 2. Modo Base de Datos (PostgreSQL)
Para integración con sistemas contables o Dashboards (Power BI). Requiere configurar el archivo `.env`.
```bash
python src/main.py "D:/mis_facturas_xml" --output_format database
```

### 📂 Estructura de Datos Generada

| Tabla / Archivo | Contenido |
|-----------------|-----------|
| `stg_xml_headers` | Cabeceras de facturas (RUCs, Fechas, Totales). |
| `stg_xml_items` | Detalle de productos/servicios vendidos. |
| `stg_sire_ventas` | Registro de Ventas (RVIE) normalizado. |
| `stg_sire_compras` | Registro de Compras (RCE) normalizado. |
| `stg_tra` | Datos de trabajadores del T-Registro. |

---

### 👤 Author / Autor

**Giusseppe Marchan**
*Bachiller en Economía & Desarrollador de Software*
*8+ años de experiencia en Contabilidad y Finanzas.*

> *Este proyecto demuestra la intersección entre la normativa contable rigurosa y las prácticas modernas de desarrollo de software.*
