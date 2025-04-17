from .base_processor import BaseXMLProcessor
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET

class FacturaProcessor(BaseXMLProcessor):
    def __init__(self, logger):
        super().__init__(logger)
        # Definir los namespaces utilizados en los documentos XML de SUNAT
        self.NAMESPACES = {
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
            'udt': 'urn:un:unece:uncefact:data:specification:UnqualifiedDataTypesSchemaModule:2',
            'qdt': 'urn:oasis:names:specification:ubl:schema:xsd:QualifiedDatatypes-2',
            'sac': 'urn:sunat:names:specification:ubl:peru:schema:xsd:SunatAggregateComponents-1'
        }

    def safe_find_text(self, element, xpath, namespaces=None):
        """Extrae texto de un elemento XML con manejo seguro de nulos"""
        found = element.find(xpath, namespaces)
        return found.text if found is not None else None

    def safe_find_attr(self, element, xpath, attr_name, namespaces=None):
        """Extrae un atributo de un elemento XML con manejo seguro de nulos"""
        found = element.find(xpath, namespaces)
        return found.get(attr_name) if found is not None else None

    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Procesa un archivo XML de Factura y extrae sus datos principales"""
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_path}")
        
        try:
            if not self.validate_xml(file_path):
                return pd.DataFrame()
                
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Logging para depuración
            root_tag = root.tag
            root_attrs = root.attrib
            self.log_operation("Procesamiento", "Info", f"Root element: {root_tag}, Root attributes: {root_attrs}")
            
            # Extraer todos los datos usando los namespaces adecuados
            invoice_data = {}
            
            # Información básica de la factura
            invoice_data['numero'] = self.safe_find_text(root, './/cbc:ID', self.NAMESPACES)
            invoice_data['fecha_emision'] = self.safe_find_text(root, './/cbc:IssueDate', self.NAMESPACES)
            invoice_data['tipo_documento'] = self.safe_find_text(root, './/cbc:InvoiceTypeCode', self.NAMESPACES)
            invoice_data['moneda'] = self.safe_find_attr(root, './/cbc:DocumentCurrencyCode', 'currencyID', self.NAMESPACES) or self.safe_find_text(root, './/cbc:DocumentCurrencyCode', self.NAMESPACES)
            
            # Información del proveedor (emisor)
            supplier = root.find('.//cac:AccountingSupplierParty', self.NAMESPACES)
            if supplier is not None:
                invoice_data['ruc_emisor'] = self.safe_find_text(supplier, './/cbc:ID', self.NAMESPACES)
                invoice_data['nombre_emisor'] = self.safe_find_text(supplier, './/cbc:RegistrationName', self.NAMESPACES)
                
                # Dirección del proveedor
                supplier_address = supplier.find('.//cac:PostalAddress', self.NAMESPACES)
                if supplier_address is not None:
                    invoice_data['direccion_emisor'] = self.safe_find_text(supplier_address, './/cbc:StreetName', self.NAMESPACES)
                    invoice_data['ciudad_emisor'] = self.safe_find_text(supplier_address, './/cbc:CityName', self.NAMESPACES)
                    invoice_data['departamento_emisor'] = self.safe_find_text(supplier_address, './/cbc:CountrySubentity', self.NAMESPACES)
                    invoice_data['distrito_emisor'] = self.safe_find_text(supplier_address, './/cbc:District', self.NAMESPACES)
                    invoice_data['pais_emisor'] = self.safe_find_text(supplier_address, './/cac:Country/cbc:IdentificationCode', self.NAMESPACES)
            
            # Información del cliente (receptor)
            customer = root.find('.//cac:AccountingCustomerParty', self.NAMESPACES)
            if customer is not None:
                invoice_data['ruc_receptor'] = self.safe_find_text(customer, './/cbc:ID', self.NAMESPACES)
                invoice_data['tipo_documento_receptor'] = self.safe_find_attr(customer, './/cbc:ID', 'schemeID', self.NAMESPACES)
                invoice_data['nombre_receptor'] = self.safe_find_text(customer, './/cbc:RegistrationName', self.NAMESPACES) or self.safe_find_text(customer, './/cbc:Name', self.NAMESPACES)
                
                # Dirección del cliente
                customer_address = customer.find('.//cac:PostalAddress', self.NAMESPACES)
                if customer_address is not None:
                    invoice_data['direccion_receptor'] = self.safe_find_text(customer_address, './/cbc:StreetName', self.NAMESPACES)
                    invoice_data['ciudad_receptor'] = self.safe_find_text(customer_address, './/cbc:CityName', self.NAMESPACES)
                    invoice_data['departamento_receptor'] = self.safe_find_text(customer_address, './/cbc:CountrySubentity', self.NAMESPACES)
                    invoice_data['distrito_receptor'] = self.safe_find_text(customer_address, './/cbc:District', self.NAMESPACES)
                    invoice_data['pais_receptor'] = self.safe_find_text(customer_address, './/cac:Country/cbc:IdentificationCode', self.NAMESPACES)
            
            # Información de totales y impuestos
            # Totales
            legal_monetary = root.find('.//cac:LegalMonetaryTotal', self.NAMESPACES)
            if legal_monetary is not None:
                invoice_data['importe_total'] = self.safe_find_text(legal_monetary, './/cbc:PayableAmount', self.NAMESPACES)
                invoice_data['total_valor_venta'] = self.safe_find_text(legal_monetary, './/cbc:LineExtensionAmount', self.NAMESPACES)
                invoice_data['total_precio_venta'] = self.safe_find_text(legal_monetary, './/cbc:TaxInclusiveAmount', self.NAMESPACES)
                invoice_data['total_descuentos'] = self.safe_find_text(legal_monetary, './/cbc:AllowanceTotalAmount', self.NAMESPACES)
                invoice_data['total_cargos'] = self.safe_find_text(legal_monetary, './/cbc:ChargeTotalAmount', self.NAMESPACES)
                invoice_data['total_anticipos'] = self.safe_find_text(legal_monetary, './/cbc:PrepaidAmount', self.NAMESPACES)
                invoice_data['total_a_pagar'] = self.safe_find_text(legal_monetary, './/cbc:PayableAmount', self.NAMESPACES)
            
            # Detalles de impuestos
            tax_total = root.find('.//cac:TaxTotal', self.NAMESPACES)
            if tax_total is not None:
                invoice_data['total_impuestos'] = self.safe_find_text(tax_total, './/cbc:TaxAmount', self.NAMESPACES)
                
                # Extraer información detallada de cada tipo de impuesto
                for tax_subtotal in tax_total.findall('.//cac:TaxSubtotal', self.NAMESPACES):
                    tax_scheme = tax_subtotal.find('.//cac:TaxScheme/cbc:ID', self.NAMESPACES)
                    if tax_scheme is not None:
                        tax_code = tax_scheme.text
                        tax_amount = self.safe_find_text(tax_subtotal, './/cbc:TaxAmount', self.NAMESPACES)
                        
                        # Asignar valores según el tipo de impuesto
                        if tax_code == '1000':  # IGV
                            invoice_data['total_igv'] = tax_amount
                        elif tax_code == '2000':  # ISC
                            invoice_data['total_isc'] = tax_amount
                        elif tax_code == '9999':  # Otros tributos
                            invoice_data['total_otros_tributos'] = tax_amount
            
            # Procesar líneas de factura
            invoice_lines = []
            lines = root.findall('.//cac:InvoiceLine', self.NAMESPACES)
            
            if not lines:
                # Si no hay líneas, retornar un DataFrame solo con los datos de la factura
                self.log_operation("Procesamiento", "Advertencia", f"No se encontraron líneas de detalle en: {file_path}")
                return pd.DataFrame([invoice_data])
            
            # Si hay líneas, para cada línea de factura crear una fila completa con todos los datos
            for line in lines:
                # Crear una copia de los datos de la factura para esta línea
                line_row = invoice_data.copy()
                
                # Añadir los datos específicos de esta línea
                line_row['linea_id'] = self.safe_find_text(line, './/cbc:ID', self.NAMESPACES)
                line_row['cantidad'] = self.safe_find_text(line, './/cbc:InvoicedQuantity', self.NAMESPACES)
                line_row['unidad'] = self.safe_find_attr(line, './/cbc:InvoicedQuantity', 'unitCode', self.NAMESPACES)
                line_row['descripcion'] = self.safe_find_text(line, './/cbc:Description', self.NAMESPACES)
                
                # Información de precio unitario
                price_amount = line.find('.//cac:Price/cbc:PriceAmount', self.NAMESPACES)
                if price_amount is not None:
                    line_row['precio_unitario'] = price_amount.text
                    line_row['moneda_precio_unitario'] = price_amount.get('currencyID')
                
                # Valores de precio de venta
                pricing_reference = line.find('.//cac:PricingReference', self.NAMESPACES)
                if pricing_reference is not None:
                    alternative_price = pricing_reference.find('.//cac:AlternativeConditionPrice', self.NAMESPACES)
                    if alternative_price is not None:
                        line_row['precio_venta_unitario'] = self.safe_find_text(alternative_price, './/cbc:PriceAmount', self.NAMESPACES)
                        line_row['tipo_precio_venta'] = self.safe_find_text(alternative_price, './/cbc:PriceTypeCode', self.NAMESPACES)
                
                # Valores totales de la línea
                line_row['subtotal'] = self.safe_find_text(line, './/cbc:LineExtensionAmount', self.NAMESPACES)
                
                # Información de impuestos específicos de la línea
                tax_totals = line.findall('.//cac:TaxTotal', self.NAMESPACES)
                for tax_total in tax_totals:
                    line_row['linea_impuesto_total'] = self.safe_find_text(tax_total, './/cbc:TaxAmount', self.NAMESPACES)
                    
                    tax_subtotals = tax_total.findall('.//cac:TaxSubtotal', self.NAMESPACES)
                    for tax_subtotal in tax_subtotals:
                        tax_category = tax_subtotal.find('.//cac:TaxCategory', self.NAMESPACES)
                        if tax_category is not None:
                            tax_scheme = tax_category.find('.//cac:TaxScheme/cbc:ID', self.NAMESPACES)
                            if tax_scheme is not None:
                                tax_code = tax_scheme.text
                                tax_amount = self.safe_find_text(tax_subtotal, './/cbc:TaxAmount', self.NAMESPACES)
                                
                                # Asignar valores según el tipo de impuesto
                                if tax_code == '1000':  # IGV
                                    line_row['linea_igv'] = tax_amount
                                    line_row['linea_codigo_igv'] = self.safe_find_text(tax_category, './/cbc:TaxExemptionReasonCode', self.NAMESPACES)
                                    line_row['linea_porcentaje_igv'] = self.safe_find_text(tax_category, './/cbc:Percent', self.NAMESPACES)
                                elif tax_code == '2000':  # ISC
                                    line_row['linea_isc'] = tax_amount
                                    line_row['linea_codigo_isc'] = self.safe_find_text(tax_category, './/cbc:TierRange', self.NAMESPACES)
                                elif tax_code == '9999':  # Otros tributos
                                    line_row['linea_otros_tributos'] = tax_amount
                
                # Añadir esta línea al listado
                invoice_lines.append(line_row)
            
            # Convertir todas las líneas a un DataFrame
            df_result = pd.DataFrame(invoice_lines)
            
            self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_path}, Líneas extraídas: {len(invoice_lines)}")
            return df_result
            
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Archivo: {file_path}, Error: {str(e)}")
            return pd.DataFrame() 