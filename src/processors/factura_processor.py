from .base_processor import BaseDocumentProcessor
from utils.xml_utils import get_xml_encoding
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
import logging

class FacturaProcessor(BaseDocumentProcessor):
    def __init__(self, logger):
        super().__init__(logger)
        self.NAMESPACES = {
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
            'sac': 'urn:sunat:names:specification:ubl:peru:schema:xsd:SunatAggregateComponents-1'
        }

    def get_db_mapping(self) -> Dict[str, Dict]:
        return {
            'header': {
                'table': 'cabeceras',
                'schema': 'public',
                'columns': {
                    'CUI': 'cui', 'numero': 'numero_documento', 'fecha_emision': 'fecha_emision',
                    'tipo_documento': 'tipo_documento_id', 'moneda': 'moneda_id', 'ruc_emisor': 'ruc_emisor',
                    'nombre_emisor': 'nombre_emisor', 'ruc_receptor': 'ruc_receptor', 'nombre_receptor': 'nombre_receptor',
                    'importe_total': 'importe_total', 'total_igv': 'total_igv', 'total_isc': 'total_isc',
                    'total_otros_tributos': 'total_otros_tributos',
                }
            },
            'lines': {
                'table': 'lineas', 'schema': 'public',
                'columns': {
                    'CUI': 'cui_relacionado', 'linea_id': 'linea_id', 'cantidad': 'cantidad',
                    'unidad': 'unidad_medida', 'descripcion': 'descripcion', 'precio_unitario': 'precio_unitario',
                    'subtotal': 'subtotal', 'linea_igv': 'igv',
                }
            },
            'payment_terms': {
                'table': 'pagos', 'schema': 'public',
                'columns': {
                    'CUI': 'cui_relacionado', 'forma_pago_id': 'forma_pago_id', 'monto_pago': 'monto',
                    'moneda_pago': 'moneda_id', 'fecha_vencimiento': 'fecha_vencimiento',
                }
            }
        }

    def safe_find_text(self, element, xpath, namespaces=None):
        found = element.find(xpath, namespaces)
        return found.text if found is not None else None

    def safe_find_attr(self, element, xpath, attr_name, namespaces=None):
        found = element.find(xpath, namespaces)
        return found.get(attr_name) if found is not None else None

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        file_name = Path(file_path).name
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_name}")

        try:
            encoding = get_xml_encoding(file_path)
            with open(file_path, 'r', encoding=encoding) as f:
                xml_content = f.read()
            
            root = ET.fromstring(xml_content)
            
            result = {'header': pd.DataFrame(), 'lines': pd.DataFrame(), 'payment_terms': pd.DataFrame()}
            
            invoice_data = {}
            invoice_data['numero'] = self.safe_find_text(root, './/cbc:ID', self.NAMESPACES)
            invoice_data['fecha_emision'] = self.safe_find_text(root, './/cbc:IssueDate', self.NAMESPACES)
            invoice_data['tipo_documento'] = self.safe_find_text(root, './/cbc:InvoiceTypeCode', self.NAMESPACES)
            invoice_data['moneda'] = self.safe_find_attr(root, './/cbc:DocumentCurrencyCode', 'currencyID', self.NAMESPACES) or self.safe_find_text(root, './/cbc:DocumentCurrencyCode', self.NAMESPACES)
            
            supplier = root.find('.//cac:AccountingSupplierParty', self.NAMESPACES)
            if supplier:
                invoice_data['ruc_emisor'] = self.safe_find_text(supplier, './/cbc:ID', self.NAMESPACES)
                invoice_data['nombre_emisor'] = self.safe_find_text(supplier, './/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName', self.NAMESPACES)

            customer = root.find('.//cac:AccountingCustomerParty', self.NAMESPACES)
            if customer:
                invoice_data['ruc_receptor'] = self.safe_find_text(customer, './/cbc:ID', self.NAMESPACES)
                invoice_data['nombre_receptor'] = self.safe_find_text(customer, './/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName', self.NAMESPACES)

            legal_monetary = root.find('.//cac:LegalMonetaryTotal', self.NAMESPACES)
            if legal_monetary:
                invoice_data['importe_total'] = self.safe_find_text(legal_monetary, './/cbc:PayableAmount', self.NAMESPACES)

            tax_total = root.find('.//cac:TaxTotal', self.NAMESPACES)
            if tax_total:
                for tax_subtotal in tax_total.findall('.//cac:TaxSubtotal', self.NAMESPACES):
                    tax_scheme = tax_subtotal.find('.//cac:TaxCategory/cac:TaxScheme/cbc:ID', self.NAMESPACES)
                    if tax_scheme is not None:
                        tax_code = tax_scheme.text
                        tax_amount = self.safe_find_text(tax_subtotal, './/cbc:TaxAmount', self.NAMESPACES)
                        if tax_code == '1000': invoice_data['total_igv'] = tax_amount
                        elif tax_code == '2000': invoice_data['total_isc'] = tax_amount
                        elif tax_code == '9999': invoice_data['total_otros_tributos'] = tax_amount
            
            cui = self._generate_cui(invoice_data)
            invoice_data['CUI'] = cui
            
            result['header'] = pd.DataFrame([invoice_data])

            lines = root.findall('.//cac:InvoiceLine', self.NAMESPACES)
            if lines:
                lines_list = [self._process_line(line, cui) for line in lines]
                result['lines'] = pd.DataFrame(lines_list)

            payment_terms = root.findall('.//cac:PaymentTerms', self.NAMESPACES)
            if payment_terms:
                pt_list = [self._process_payment_term(pt, cui) for pt in payment_terms]
                result['payment_terms'] = pd.DataFrame(pt_list)
            
            self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_name}, CUI: {cui}")
            return result
            
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Error procesando archivo: {file_name}, Error: {str(e)}", level=logging.ERROR)
            return None

    def _generate_cui(self, data):
        ruc_emisor = data.get('ruc_emisor')
        tipo_doc = data.get('tipo_documento')
        numero_factura = data.get('numero')
        if ruc_emisor and tipo_doc and numero_factura:
            try:
                return f"{hex(int(ruc_emisor))[2:].lower()}{int(tipo_doc):02d}{numero_factura.replace('-', '')}"
            except (ValueError, TypeError):
                self.logger.warning(f"No se pudo generar CUI para {numero_factura}")
        return None

    def _process_line(self, line, cui):
        line_data = {'CUI': cui}
        line_data['linea_id'] = self.safe_find_text(line, './/cbc:ID', self.NAMESPACES)
        line_data['cantidad'] = self.safe_find_text(line, './/cbc:InvoicedQuantity', self.NAMESPACES)
        line_data['unidad'] = self.safe_find_attr(line, './/cbc:InvoicedQuantity', 'unitCode', self.NAMESPACES)
        line_data['descripcion'] = self.safe_find_text(line, './/cac:Item/cbc:Description', self.NAMESPACES)
        line_data['precio_unitario'] = self.safe_find_text(line, './/cac:Price/cbc:PriceAmount', self.NAMESPACES)
        line_data['subtotal'] = self.safe_find_text(line, './/cbc:LineExtensionAmount', self.NAMESPACES)
        tax_total = line.find('.//cac:TaxTotal', self.NAMESPACES)
        if tax_total:
            tax_subtotal = tax_total.find('.//cac:TaxSubtotal', self.NAMESPACES)
            if tax_subtotal:
                line_data['linea_igv'] = self.safe_find_text(tax_subtotal, './/cbc:TaxAmount', self.NAMESPACES)
        return line_data

    def _process_payment_term(self, pt_node, cui):
        pt_data = {'CUI': cui}
        pt_data['forma_pago_id'] = self.safe_find_text(pt_node, './cbc:ID', self.NAMESPACES)
        pt_data['monto_pago'] = self.safe_find_text(pt_node, './cbc:Amount', self.NAMESPACES)
        pt_data['moneda_pago'] = self.safe_find_attr(pt_node, './cbc:Amount', 'currencyID', self.NAMESPACES)
        pt_data['fecha_vencimiento'] = self.safe_find_text(pt_node, './cbc:PaymentDueDate', self.NAMESPACES)
        return pt_data
