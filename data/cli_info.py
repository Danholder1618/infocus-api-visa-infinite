from sqlalchemy import create_engine, text, outparam
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import oracledb
import json
import uuid
import os

from utils.logging_config import setup_logger

logger = setup_logger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

class OracleClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(OracleClient, cls).__new__(cls)
            cls._instance.host = os.getenv('ORACLE_HOST')
            cls._instance.port = os.getenv('ORACLE_PORT')
            cls._instance.user = os.getenv('ORACLE_USER')
            cls._instance.password = os.getenv('ORACLE_PASSWORD')
            cls._instance.sid = os.getenv('ORACLE_SID')
            cls._instance.engine = None
            cls._instance.Session = None
            
            logger.debug(f"Oracle TNS: `{cls._instance.user}@{cls._instance.host}:{cls._instance.port}/{cls._instance.sid}`")
            
            cls._instance.create_engine()
        return cls._instance

    def create_engine(self):
        try:
            self.engine = create_engine(
                f'oracle+oracledb://colvir:ColvirAloqa@172.22.2.222:1521/CBSPROD',
                pool_recycle=1800, pool_pre_ping=True, pool_size=10, max_overflow=5
            )
            self.Session = sessionmaker(bind=self.engine)
            logger.debug("OracleDB engine created")
        except Exception:
            logger.error("Error creating engine.", exc_info=True)
            exit(1)
        
    def todict(self, result):
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]
    
    def execute(self, query, args=None, session=None, commit=False):
        try:
            if session:
                result = session.execute(text(query), args)
                if commit:
                    session.commit()
                return result
            else:
                with self.engine.connect() as conn:
                    result = conn.execute(text(query), args)
                    return result
        except Exception:
            logger.error("Error executing query.", exc_info=True)
            return None

    def fetch(self, query, args=None, as_dict=False, session=None):
        try:
            result = self.execute(query, args, session=session)
            if result is None:
                return []
            if as_dict:
                data = self.todict(result)
            else:
                data = result.fetchall()
            return data
        except Exception:
            logger.error("Error fetching data.", exc_info=True)
            return []

    def pkgconnect(self, session):
        try:
            session.execute(text("call z_116_pkgconnect.popen()"))
            session.commit()
        except Exception:
            logger.error("Error calling procedure.", exc_info=True)
            raise

    def client_by_card(self, pan):
        try:
            with self.Session() as session:
                self.pkgconnect(session)
                sql = """SELECT
                            c.code AS "client_code",
                            hst.PNAME1 as "surname",
                            hst.pname2 as "name",
                            hst.pname3 as "middle_name",
                            hst.longname AS "fullname",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426) THEN cidn.idn_num
                            ELSE NULL
                            END AS "pinfl",
                            hst.passorg AS "issuer",
                            hst.passdat AS "issue_date",
                            TO_CHAR(hst.passser) || TO_CHAR(hst.passnum) AS "serial_number",
                            hst.passtyp_id as "passtype",
                            TO_CHAR(c.birdate, 'YYYY-MM-DD') AS "birth_date",
                            (SELECT tr.ALFA_3 FROM t_reg tr WHERE tr.ID = 
                                CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "country",
                            CASE 
                                WHEN hst.passtyp_id = 364 THEN '63d37e12844c5700011a48c5'
                                WHEN hst.passtyp_id = 425 THEN '63d399c0844c5700011a48ca'
                                WHEN hst.passtyp_id = 368 THEN '63d39a5e844c5700011a48ce'
                                WHEN hst.passtyp_id = 404 THEN '63d39be5844c5700011a48dc'
                                WHEN hst.passtyp_id = 426 THEN '63d39a04844c5700011a48cb'
                                WHEN hst.passtyp_id = 370 THEN '63d39bac844c5700011a48d9'
                                ELSE NULL
                            END AS "document_types_id",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426) THEN '63d28a0051aab54cec4f65ac'
                                WHEN hst.passtyp_id IN (368, 370) THEN '63d295665454de4cfc3f4f6f'
                                ELSE NULL
                            END AS "participant_type_codes_id",
                            t_pkgval.fgetvalcodeaccid(crd.acc_dep_id, crd.acc_id) AS "currency",
                            crd.acc_code AS "acc_num",
                            crd.cardcode AS "pan",
                            crd.cardidn AS "card_idn",
                            crd.cardcode_masked AS "masked_pan",
                            passt.name AS "pass_name",
                            hst.NATIONALITY as "nationality",
                            (select tr1.ALFA_3 from t_reg tr1 where tr1.id = CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "citizenship",
                            hst.addrjur AS "address",
                            NULL AS "is_bank_client",
                            NULL AS "citizenship_id",
                            NULL AS "country_id",
                            NULL AS "district_id",
                            NULL AS "region_id",
                            NULL AS "tel"
                        FROM
                            g_cli c,
                            g_clihst hst,
                            g_cliidn cidn,
                            g_identdocdsc_std passt,
                            (SELECT cli_code,cardcode,cardcode_masked,cardidn,acc_code,acc_dep_id,acc_id FROM NV_CRD_LIST l WHERE ROWNUM <= '1000001' AND CARDCODE = :pan) crd
                        WHERE
                            c.code = crd.cli_code
                            AND c.id = hst.id
                            AND c.dep_id = hst.dep_id
                            AND cidn.id = c.id
                            AND cidn.dep_id = c.dep_id
                            AND hst.passtyp_id = passt.id
                            AND sysdate BETWEEN hst.fromdate AND hst.todate
                            AND sysdate BETWEEN cidn.fromdate AND cidn.todate
                            AND ((cidn.idn_id=763 AND hst.passtyp_id <> 368) OR (hst.passtyp_id = 368))
                    """
                data = self.fetch(sql, args={"pan": pan}, as_dict=True, session=session)
                logger.debug(f"Data of cardholder by PAN `{pan}`: ```json\n{json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True, cls=DateTimeEncoder)}```")
                return data[0] if len(data) > 0 else []
        except Exception:
            logger.error(f"Error getting card by PAN: `{pan}`", exc_info=True)
            return []

    def client_by_cardidn(self, cardidn):
        try:
            with self.Session() as session:
                logger.debug(f"Getting client by CARDIDN `{cardidn}`")
                self.pkgconnect(session)
                sql = """SELECT
                            c.code AS "client_code",
                            hst.PNAME1 as "surname",
                            hst.pname2 as "name",
                            hst.pname3 as "middle_name",
                            hst.longname AS "fullname",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426) THEN cidn.idn_num
                            ELSE NULL
                            END AS "pinfl",
                            hst.passorg AS "issuer",
                            hst.passdat AS "issue_date",
                            TO_CHAR(hst.passser) || TO_CHAR(hst.passnum) AS "serial_number",
                            hst.passtyp_id as "passtype",
                            TO_CHAR(c.birdate, 'YYYY-MM-DD') AS "birth_date",
                            (SELECT tr.ALFA_3 FROM t_reg tr WHERE tr.ID = 
                                CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "country",
                            CASE 
                                WHEN hst.passtyp_id = 364 THEN '63d37e12844c5700011a48c5'
                                WHEN hst.passtyp_id = 425 THEN '63d399c0844c5700011a48ca'
                                WHEN hst.passtyp_id = 368 THEN '63d39a5e844c5700011a48ce'
                                WHEN hst.passtyp_id = 404 THEN '63d39be5844c5700011a48dc'
                                WHEN hst.passtyp_id = 426 THEN '63d39a04844c5700011a48cb'
                                WHEN hst.passtyp_id = 370 THEN '63d39bac844c5700011a48d9'
                                WHEN hst.passtyp_id = 445 THEN '642429c72c12830001cd506a'
                                ELSE NULL
                            END AS "document_types_id",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426, 445) THEN '63d28a0051aab54cec4f65ac'
                                WHEN hst.passtyp_id IN (368, 370) THEN '63d295665454de4cfc3f4f6f'
                                ELSE NULL
                            END AS "participant_type_codes_id",
                            t_pkgval.fgetvalcodeaccid(crd.acc_dep_id, crd.acc_id) AS "currency",
                            crd.acc_code AS "acc_num",
                            crd.cardcode AS "pan",
                            crd.cardidn AS "card_idn",
                            crd.cardcode_masked AS "masked_pan",
                            passt.name AS "pass_name",
                            hst.NATIONALITY as "nationality",
                            (select tr1.ALFA_3 from t_reg tr1 where tr1.id = CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "citizenship",
                            hst.addrjur AS "address",
                            NULL AS "is_bank_client",
                            NULL AS "citizenship_id",
                            NULL AS "country_id",
                            NULL AS "district_id",
                            NULL AS "region_id",
                            NULL AS "tel"
                        FROM
                            g_cli c,
                            g_clihst hst,
                            g_cliidn cidn,
                            g_identdocdsc_std passt,
                            (SELECT cli_code,cardcode,cardcode_masked,cardidn,acc_code,acc_dep_id,acc_id FROM NV_CRD_LIST l WHERE ROWNUM <= '1000001' AND CARDIDN = :cardidn) crd
                        WHERE
                            c.code = crd.cli_code
                            AND c.id = hst.id
                            AND c.dep_id = hst.dep_id
                            AND cidn.id = c.id
                            AND cidn.dep_id = c.dep_id
                            AND hst.passtyp_id = passt.id
                            AND sysdate BETWEEN hst.fromdate AND hst.todate
                            AND sysdate BETWEEN cidn.fromdate AND cidn.todate
                            AND ((cidn.idn_id=763 AND hst.passtyp_id <> 368) OR (hst.passtyp_id = 368) OR (hst.passtyp_id=370))
                    """
                data = self.fetch(sql, args={"cardidn": cardidn}, as_dict=True, session=session)
                logger.debug(f"Data of cardholder by CARDIDN `{cardidn}`: ```json\n{json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True, cls=DateTimeEncoder)}```")
                return data[0] if len(data) > 0 else []
        except Exception:
            logger.error(f"Error getting card by CARDIDN `{cardidn}`", exc_info=True)
            return []

    def client_by_acc_code(self, acc_code):
        try:
            with self.Session() as session:
                logger.debug(f"Getting client by CARDIDN `{acc_code}`")
                self.pkgconnect(session)
                sql = """SELECT
                            c.code AS "client_code",
                            hst.PNAME1 as "surname",
                            hst.pname2 as "name",
                            hst.pname3 as "middle_name",
                            hst.longname AS "fullname",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426) THEN cidn.idn_num
                            ELSE NULL
                            END AS "pinfl",
                            hst.passorg AS "issuer",
                            hst.passdat AS "issue_date",
                            TO_CHAR(hst.passser) || TO_CHAR(hst.passnum) AS "serial_number",
                            hst.passtyp_id as "passtype",
                            TO_CHAR(c.birdate, 'YYYY-MM-DD') AS "birth_date",
                            (SELECT tr.ALFA_3 FROM t_reg tr WHERE tr.ID = 
                                CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "country",
                            CASE 
                                WHEN hst.passtyp_id = 364 THEN '63d37e12844c5700011a48c5'
                                WHEN hst.passtyp_id = 425 THEN '63d399c0844c5700011a48ca'
                                WHEN hst.passtyp_id = 368 THEN '63d39a5e844c5700011a48ce'
                                WHEN hst.passtyp_id = 404 THEN '63d39be5844c5700011a48dc'
                                WHEN hst.passtyp_id = 426 THEN '63d39a04844c5700011a48cb'
                                WHEN hst.passtyp_id = 370 THEN '63d39bac844c5700011a48d9'
                                WHEN hst.passtyp_id = 445 THEN '642429c72c12830001cd506a'
                                ELSE NULL
                            END AS "document_types_id",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426, 445) THEN '63d28a0051aab54cec4f65ac'
                                WHEN hst.passtyp_id IN (368, 370) THEN '63d295665454de4cfc3f4f6f'
                                ELSE NULL
                            END AS "participant_type_codes_id",
                            t_pkgval.fgetvalcodeaccid(crd.acc_dep_id, crd.acc_id) AS "currency",
                            crd.acc_code AS "acc_num",
                            crd.cardcode AS "pan",
                            crd.cardidn AS "card_idn",
                            crd.cardcode_masked AS "masked_pan",
                            passt.name AS "pass_name",
                            hst.NATIONALITY as "nationality",
                            (select tr1.ALFA_3 from t_reg tr1 where tr1.id = CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "citizenship",
                            hst.addrjur AS "address",
                            NULL AS "is_bank_client",
                            NULL AS "citizenship_id",
                            NULL AS "country_id",
                            NULL AS "district_id",
                            NULL AS "region_id",
                            NULL AS "tel"
                        FROM
                            g_cli c,
                            g_clihst hst,
                            g_cliidn cidn,
                            g_identdocdsc_std passt,
                            (SELECT cli_code,cardcode,cardcode_masked,cardidn,acc_code,acc_dep_id,acc_id FROM NV_CRD_LIST l WHERE ROWNUM <= '1000001' AND acc_code = :acc_code) crd
                        WHERE
                            c.code = crd.cli_code
                            AND c.id = hst.id
                            AND c.dep_id = hst.dep_id
                            AND cidn.id = c.id
                            AND cidn.dep_id = c.dep_id
                            AND hst.passtyp_id = passt.id
                            AND sysdate BETWEEN hst.fromdate AND hst.todate
                            AND sysdate BETWEEN cidn.fromdate AND cidn.todate
                            AND ((cidn.idn_id=763 AND hst.passtyp_id <> 368) OR (hst.passtyp_id = 368) OR (hst.passtyp_id=370))
                    """
                data = self.fetch(sql, args={"acc_code": acc_code}, as_dict=True, session=session)
                logger.debug(f"Data of cardholder by acc_code `{acc_code}`: ```json\n{json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True, cls=DateTimeEncoder)}```")
                return data[0] if len(data) > 0 else []
        except Exception:
            logger.error(f"Error getting card by acc_code `{acc_code}`", exc_info=True)
            return []

    def client_by_code(self, cli_code):
        try:
            with self.Session() as session:
                logger.debug(f"Getting client by CLI_CODE `{cli_code}`")
                self.pkgconnect(session)
                sql = """SELECT
                            c.code AS "client_code",
                            hst.PNAME1 as "surname",
                            hst.pname2 as "name",
                            hst.pname3 as "middle_name",
                            hst.longname AS "fullname",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426) THEN cidn.idn_num
                            ELSE NULL
                            END AS "pinfl",
                            hst.passorg AS "issuer",
                            hst.passdat AS "issue_date",
                            TO_CHAR(hst.passser) || TO_CHAR(hst.passnum) AS "serial_number",
                            hst.passtyp_id as "passtype",
                            TO_CHAR(c.birdate, 'YYYY-MM-DD') AS "birth_date",
                            (SELECT tr.ALFA_3 FROM t_reg tr WHERE tr.ID = 
                                CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "country",
                            CASE 
                                WHEN hst.passtyp_id = 364 THEN '63d37e12844c5700011a48c5'
                                WHEN hst.passtyp_id = 425 THEN '63d399c0844c5700011a48ca'
                                WHEN hst.passtyp_id = 368 THEN '63d39a5e844c5700011a48ce'
                                WHEN hst.passtyp_id = 404 THEN '63d39be5844c5700011a48dc'
                                WHEN hst.passtyp_id = 426 THEN '63d39a04844c5700011a48cb'
                                WHEN hst.passtyp_id = 370 THEN '63d39bac844c5700011a48d9'
                                ELSE NULL
                            END AS "document_types_id",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426) THEN '63d28a0051aab54cec4f65ac'
                                WHEN hst.passtyp_id IN (368, 370) THEN '63d295665454de4cfc3f4f6f'
                                ELSE NULL
                            END AS "participant_type_codes_id",
                            t_pkgval.fgetvalcodeaccid(crd.acc_dep_id, crd.acc_id) AS "currency",
                            crd.acc_code AS "acc_num",
                            crd.cardcode AS "pan",
                            crd.cardidn AS "card_idn",
                            crd.cardcode_masked AS "masked_pan",
                            passt.name AS "pass_name",
                            hst.NATIONALITY as "nationality",
                            (select tr1.ALFA_3 from t_reg tr1 where tr1.id = CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "citizenship",
                            hst.addrjur AS "address",
                            NULL AS "is_bank_client",
                            NULL AS "citizenship_id",
                            NULL AS "country_id",
                            NULL AS "district_id",
                            NULL AS "region_id",
                            NULL AS "tel"
                        FROM
                            g_cli c,
                            g_clihst hst,
                            g_cliidn cidn,
                            g_identdocdsc_std passt,
                            (SELECT cli_code,cardcode,cardcode_masked,cardidn,acc_code,acc_dep_id,acc_id FROM NV_CRD_LIST l WHERE ROWNUM <= '1000001' AND cli_code = :cli_code) crd
                        WHERE
                            c.code = crd.cli_code
                            AND c.id = hst.id
                            AND c.dep_id = hst.dep_id
                            AND cidn.id = c.id
                            AND cidn.dep_id = c.dep_id
                            AND hst.passtyp_id = passt.id
                            AND sysdate BETWEEN hst.fromdate AND hst.todate
                            AND sysdate BETWEEN cidn.fromdate AND cidn.todate
                            AND ((cidn.idn_id=763 AND hst.passtyp_id <> 368) OR (hst.passtyp_id = 368))
                    """
                data = self.fetch(sql, args={"cli_code": cli_code}, as_dict=True, session=session)
                logger.debug(f"Data of cardholder by CLI_CODE `{cli_code}`: ```json\n{json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True, cls=DateTimeEncoder)}```")
                return data[0] if len(data) > 0 else []
        except Exception:
            logger.error(f"Error getting card by CLI_CODE `{cli_code}`", exc_info=True)
            return []
    
    def client_by_client_code(self, cli_code):
        try:
            with self.Session() as session:
                logger.debug(f"Getting client by CLI_CODE `{cli_code}`")
                self.pkgconnect(session)
                sql = """SELECT
                            c.code AS "client_code",
                            hst.PNAME1 as "surname",
                            hst.pname2 as "name",
                            hst.pname3 as "middle_name",
                            hst.longname AS "fullname",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404) THEN cidn.idn_num
                            ELSE NULL
                            END AS "pinfl",
                            hst.passorg AS "issuer",
                            TO_CHAR(hst.passdat, 'YYYY-MM-DD') AS "issue_date",
                            TO_CHAR(hst.passser) || TO_CHAR(hst.passnum) AS "serial_number",
                            hst.passtyp_id as "passtype",
                            TO_CHAR(c.birdate, 'YYYY-MM-DD') AS "birth_date",
                            (SELECT tr.ALFA_3 FROM t_reg tr WHERE tr.ID = 
                                CASE WHEN hst.CITIZ_ID IS NULL THEN hst.REG_ID ELSE hst.CITIZ_ID END) AS "country",
                            CASE 
                                WHEN hst.passtyp_id = 364 THEN '63d37e12844c5700011a48c5'
                                WHEN hst.passtyp_id = 425 THEN '63d399c0844c5700011a48ca'
                                WHEN hst.passtyp_id = 368 THEN '63d39a5e844c5700011a48ce'
                                WHEN hst.passtyp_id = 404 THEN '63d39be5844c5700011a48dc'
                                WHEN hst.passtyp_id = 426 THEN '63d39a04844c5700011a48cb'
                                WHEN hst.passtyp_id = 370 THEN '63d39bac844c5700011a48d9'
                                WHEN hst.passtyp_id = 445 THEN '642429c72c12830001cd506a'
                                ELSE NULL
                            END AS "document_types_id",
                            CASE 
                                WHEN hst.passtyp_id IN (364, 425, 404, 426, 445) THEN '63d28a0051aab54cec4f65ac'
                                WHEN hst.passtyp_id IN (368, 370) THEN '63d295665454de4cfc3f4f6f'
                                ELSE NULL
                            END AS "participant_type_codes_id",
                            passt.name AS "pass_name",
                            hst.NATIONALITY as "nationality",
                            (select tr1.ALFA_3 from t_reg tr1 where tr1.id = hst.REG_ID) AS "citizenship",
                            hst.addrjur AS "address"
                        FROM
                            g_cli c,
                            g_clihst hst,
                            g_cliidn cidn,
                            g_identdocdsc_std passt
                        WHERE
                            c.code = :cli_code
                            AND c.id = hst.id
                            AND c.dep_id = hst.dep_id
                            AND cidn.id = c.id
                            AND cidn.dep_id = c.dep_id
                            AND hst.passtyp_id = passt.id
                            AND sysdate BETWEEN hst.fromdate AND hst.todate
                            AND sysdate BETWEEN cidn.fromdate AND cidn.todate
                            AND ((cidn.idn_id=763 AND hst.passtyp_id <> 368) OR (hst.passtyp_id = 368) OR (hst.passtyp_id=370))
                    """
                data = self.fetch(sql, args={"cli_code": cli_code}, as_dict=True, session=session)
                # if len(data) > 0:
                logger.debug(f"Data of cardholder by CLI_CODE `{cli_code}`: ```json\n{json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True, cls=DateTimeEncoder)}```")
                return data[0] if len(data) > 0 else None
        except Exception:
            logger.error(f"Error getting card by CLI_CODE `{cli_code}`", exc_info=True)
            return None

    def entity_by_code(self, cli_code):
        try:
            with self.Session() as session:
                logger.debug(f"Getting client by CLI_CODE `{cli_code}`")
                self.pkgconnect(session)
                sql = """SELECT 
                            c.code AS code,
                            (
                                SELECT v.value AS val
                                FROM u_uniref r, g_clirefval v
                                WHERE r.id = v.ref_id + 0
                                AND v.id = c.id
                                AND v.dep_id = c.dep_id
                                AND r.code = 'UZ_CLITYPE'
                            ) AS participant_type,
                            c.llongname AS org_name,
                            c.tax_name AS tax_name,
                            c.tax_code AS tax_code,
                            c.addrjur AS address,
                            c.BIRTHDATE AS birthdate,
                            c.okpo AS okpo,
                            (SELECT alfa_3 FROM t_reg WHERE alfa_3 = c.reg_code OR dig_code = c.reg_code OR code = c.reg_code) AS country,
                            c.type_name AS type_name,
                            CASE
                                WHEN c.taxcode_plain IS NULL AND c.type_name = 'ИП' THEN cidn.idn_num
                                ELSE c.taxcode_plain
                            END AS stir,
                            NVL(cp.pname1, ca.pname1) AS beneficiary_first_name,
                            NVL(cp.pname2, ca.pname2) AS beneficiary_last_name,
                            (SELECT alfa_3 FROM t_reg r WHERE r.id = NVL(ca.reg_id, cp.citiz_id)) AS beneficiary_citizenship,
                            CASE
                                WHEN (SELECT alfa_3 FROM t_reg r WHERE r.id = NVL(ca.reg_id, cp.citiz_id)) = 'UZB' THEN '63d28a0051aab54cec4f65ac'
                                ELSE '63d295665454de4cfc3f4f6f'
                            END AS beneficiary_participant_type,
                            NVL(
                                (SELECT code FROM g_cli gc WHERE cp.prsdep_id = gc.dep_id and cp.prs_id = gc.id),
                                (SELECT code FROM g_cli gc WHERE ca.CLI_DEP_ID = gc.dep_id and ca.CLI_ID = gc.id)
                            ) AS dir_code,
                            NVL(
                                (SELECT idn_num FROM g_cli gc, g_cliidn cidn WHERE cp.prsdep_id = gc.dep_id and cp.prs_id = gc.id And cidn.id = gc.id and cidn.dep_id = gc.dep_id AND IDN_ID=763 AND idn_num <> '00000000000000' AND ROWNUM = 1),
                                (SELECT idn_num FROM g_cli gc, g_cliidn cidn WHERE ca.CLI_DEP_ID = gc.dep_id and ca.CLI_ID = gc.id And cidn.id = gc.id and cidn.dep_id = gc.dep_id AND IDN_ID=763 AND idn_num <> '00000000000000' AND  ROWNUM = 1)
                            ) AS dir_idn
                        FROM
                            gv_cli c
                        LEFT JOIN
                            g_cliadmlst ca ON c.id = ca.id AND c.dep_id = ca.dep_id
                        LEFT JOIN
                            (
                                SELECT
                                    *
                                FROM
                                    (
                                        SELECT
                                            c.code,
                                            c.id,
                                            c.dep_id,
                                            pname1,
                                            pname2,
                                            pname3,
                                            citiz_id,
                                            prs_id,
                                            prsdep_id /*, (select code from g_cli where ca.prs_id = id AND ca.prsdep_id = dep_id)*/
                                        FROM
                                                g_cliauthprs ca
                                            JOIN g_cli    c ON c.id = ca.id
                                            JOIN g_clihst ch ON ca.prs_id = ch.id
                                                                AND ca.prsdep_id = ch.dep_id
                                        WHERE
                                            c.code = :clicode
                                        ORDER BY
                                            TO_NUMBER(nsign)
                                    )
                                WHERE
                                    ROWNUM = 1
                            ) cp ON cp.code = c.code
                        LEFT JOIN
                            g_cliidn cidn ON cidn.id = c.id and cidn.dep_id = c.dep_id AND IDN_ID=763
                        WHERE
                            (c.taxcode_plain IS NULL OR c.type_name <> 'ИП') AND
                            c.code = :clicode
                            AND ROWNUM = 1
                        ORDER BY
                            ca.nord DESC"""
                data = self.fetch(sql, args={"clicode": cli_code}, as_dict=True, session=session)
                logger.debug(f"Data of entity by CLI_CODE `{cli_code}`: ```json\n{json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True, cls=DateTimeEncoder)}```")
                return data[0] if len(data) > 0 else []
        except Exception:
            logger.error(f"Error getting entity by CLI_CODE `{cli_code}`", exc_info=True)
            return []
    
    def mts_by_id(self, id):
        try:
            with self.Session() as session:
                self.pkgconnect(session)
                sql = """SELECT * FROM (
                            SELECT
                                id, cha_code, cha_name, credit, rcv_type, rcv_name,
                                cli_resident, cli_res_code, cli_res_alpha3, cli_res_name,
                                cli_name, cli_lastname, cli_firstname, cli_middlename,
                                cli_passtype, cli_passnum, cli_passser, cli_passorg, cli_passodate, cli_dob,
                                cli_address, cli_addr_name, cli_addr_alpha2, cli_addr_alpha3,
                                cli_addr_street, cli_addr_city, cli_pin, cli_phone,
                                cre_resident, cre_res_code, cre_res_alpha3, cre_res_name,
                                cre_name, cre_lastname, cre_firstname, cre_middlename,
                                cre_passtype, cre_passnum, cre_passser, cre_passorg, cre_passodate, cre_dob,
                                cre_address, cre_addr_name, cre_addr_alpha2, cre_addr_alpha3,
                                cre_addr_street, cre_addr_city, cre_pin, cre_phone
                            FROM (
                                SELECT
                                    id, cha_code, cha_name, infl AS credit, rcvtype AS rcv_type, rcvtype_name AS rcv_name,
                                    residfl_cl_name AS cli_resident,
                                    regres_code_cl AS cli_res_code,
                                    (SELECT alfa_3 FROM t_reg WHERE alfa_3 = regres_code_cl OR dig_code = regres_code_cl OR code = regres_code_cl) AS cli_res_alpha3,
                                    regres_name_cl AS cli_res_name,
                                    txt_pay AS cli_name, pname1 AS cli_lastname, pname2 AS cli_firstname, pname3 AS cli_middlename,
                                    passtyp_id_cl AS cli_passtype, passnum_cl AS cli_passnum, passser_cl AS cli_passser,
                                    passorg_cl AS cli_passorg, passdat_cl AS cli_passodate, dbirth_cl AS cli_dob,
                                    address_cl AS cli_address, cli_reg_name AS cli_addr_name, cli_reg_code AS cli_addr_alpha2,
                                    (SELECT alfa_3 FROM t_reg WHERE alfa_3 = cli_reg_code OR dig_code = cli_reg_code OR code = cli_reg_code) AS cli_addr_alpha3,
                                    street_cl AS cli_addr_street, city_cl AS cli_addr_city, ident_num_cl AS cli_pin, phone_cl AS cli_phone,
                                    residfl_cr_name AS cre_resident,
                                    regres_code_cr AS cre_res_code,
                                    (SELECT alfa_3 FROM t_reg WHERE alfa_3 = regres_code_cr OR dig_code = regres_code_cr OR code = regres_code_cr) AS cre_res_alpha3,
                                    regres_name_cr AS cre_res_name,
                                    txt_ben AS cre_name, pname1_cr AS cre_lastname, pname2_cr AS cre_firstname, pname3_cr AS cre_middlename,
                                    passtyp_id_cr AS cre_passtype, passnum_cr AS cre_passnum, passser_cr AS cre_passser,
                                    passorg_cr AS cre_passorg, passdat_cr AS cre_passodate, dbirth_cr AS cre_dob,
                                    address_cr AS cre_address, reg_name AS cre_addr_name, reg_code AS cre_addr_alpha2, reg_code_a3 AS cre_addr_alpha3,
                                    street_cr AS cre_addr_street, city_cr AS cre_addr_city, ident_num_cr AS cre_pin, phone_cr AS cre_phone
                                FROM mv_mts_detail
                                WHERE id = :id

                                UNION ALL

                                SELECT
                                    id, cha_code, cha_name, infl AS credit, rcvtype AS rcv_type, rcvtype_name AS rcv_name,
                                    CASE 
                                        WHEN CLI_RESIDFL = '1' THEN 'Резидент'
                                        WHEN CLI_RESIDFL = '0' THEN 'Не резидент'
                                        ELSE TO_CHAR(CLI_RESIDFL)
                                    END AS cli_resident,
                                    CLI_REG_CODE AS cli_res_code,
                                    (SELECT alfa_3 FROM t_reg WHERE alfa_3 = CLI_REG_CODE OR dig_code = CLI_REG_CODE OR code = CLI_REG_CODE) AS cli_res_alpha3,
                                    CLI_REG_NAME AS cli_res_name,
                                    txt_pay AS cli_name, pname1 AS cli_lastname, pname2 AS cli_firstname, pname3 AS cli_middlename,
                                    passtyp_id_cl AS cli_passtype, passnum_cl AS cli_passnum, passser_cl AS cli_passser,
                                    passorg_cl AS cli_passorg, passdat_cl AS cli_passodate, NULL AS cli_dob,
                                    address_cl AS cli_address, cli_reg_name AS cli_addr_name, cli_reg_code AS cli_addr_alpha2,
                                    (SELECT alfa_3 FROM t_reg WHERE alfa_3 = cli_reg_code OR dig_code = cli_reg_code OR code = cli_reg_code) AS cli_addr_alpha3,
                                    NULL AS cli_addr_street, NULL AS cli_addr_city, ident_num_cl AS cli_pin, phone_cl AS cli_phone,
                                    CASE 
                                        WHEN RESIDFL_CR = '1' THEN 'Резидент'
                                        WHEN RESIDFL_CR = '0' THEN 'Не резидент'
                                        ELSE RESIDFL_CR
                                    END AS cre_resident,
                                    NULL AS cre_res_code, NULL AS cre_res_alpha3, NULL AS cre_res_name,
                                    txt_ben AS cre_name, pname1_cr AS cre_lastname, pname2_cr AS cre_firstname, pname3_cr AS cre_middlename,
                                    passtyp_id_cr AS cre_passtype, passnum_cr AS cre_passnum, passser_cr AS cre_passser,
                                    passorg_cr AS cre_passorg, passdat_cr AS cre_passodate, NULL AS cre_dob,
                                    address_cr AS cre_address, reg_name AS cre_addr_name, reg_code AS cre_addr_alpha2, reg_code_a3 AS cre_addr_alpha3,
                                    NULL AS cre_addr_street, NULL AS cre_addr_city, ident_num_cr AS cre_pin, phone_cr AS cre_phone
                                FROM mv_payrdtl_all
                                WHERE id = :id
                            )
                        )
                        WHERE ROWNUM = 1"""
                data = self.fetch(sql, args={"id": id}, as_dict=True, session=session)
                return data[0] if len(data) > 0 else []
        except Exception:
            logger.error(f"Error getting mts by id `{id}`", exc_info=True)
            return []
    
    def base_value(self, date = '2024-05-20'):
        try:
            with self.Session() as session:
                self.pkgconnect(session)
                sql = """SELECT
                        T.SOLUTION AS "base_amount"
                        FROM
                        C_TBLVAL V,
                        (
                            SELECT
                            CR.ID,
                            NVL(CS.LONGNAME, CR.LONGNAME) AS LONGNAME,
                            CR.NPP,
                            CR.NORD_COL,
                            CR.NORD,
                            CR.ATR_TYPE,
                            CR.R_NAME,
                            C_PKGDECTBL.FCANONICAL2VALUE(CR.SOLUTION, CR.ID, CR.REF_SOL) SOLUTION
                            FROM
                            (
                                WITH C AS (
                                SELECT
                                    *
                                FROM
                                    C_TBLCOL C
                                WHERE
                                    C .ID = '27638'
                                ),
                                R AS (
                                SELECT
                                    *
                                FROM
                                    C_TBLROW R
                                WHERE
                                    R.ID = '27638'
                                ),
                                L AS (
                                SELECT
                                    *
                                FROM
                                    C_TBLSLT L
                                WHERE
                                    L.ID = '27638'
                                ),
                                D AS (
                                SELECT
                                    *
                                FROM
                                    C_DECTBL_STD D
                                WHERE
                                    D.ID = '27638'
                                )
                                SELECT
                                NVL(C .ID, R.ID) AS ID,
                                C .LONGNAME,
                                R.NPP,
                                C .NORD AS NORD_COL,
                                R.NORD,
                                C .ATR_TYPE,
                                R.LONGNAME AS R_NAME,
                                DECODE(
                                    D.MLSL,
                                    '1',
                                    NVL(L.SOLUTION, R.SOLUTION),
                                    R.SOLUTION
                                ) SOLUTION,
                                C .SBJ_ID,
                                D.REF_SOL
                                FROM
                                C
                                LEFT JOIN D ON C .ID = D.ID FULL
                                OUTER JOIN R ON C .ID = R.ID
                                LEFT JOIN L ON C .ID = L.ID
                                AND R.NORD = L.NORD
                            ) CR,
                            C_SBJCONCEPT CS
                            WHERE
                            CS.ID(+) = CR.SBJ_ID
                        ) T
                        WHERE
                        V.ID(+) = T.ID
                        AND V.NORD(+) = T.NORD
                        AND V.NORD_COL(+) = T.NORD_COL
                        AND T.NPP IS NOT NULL
                        AND V.ID(+) = '27638'
                        AND (TO_DATE(:dt, 'YYYY-MM-DD') BETWEEN TO_DATE(C_PKGDECTBL.FCANONICAL2VALUE(V.LF_VALUE, T.ATR_TYPE), 'DD.MM.YYYY') AND NVL(TO_DATE(C_PKGDECTBL.FCANONICAL2VALUE(V.RG_VALUE, T.ATR_TYPE), 'DD.MM.YYYY'), TO_DATE('9999-12-31', 'YYYY-MM-DD')))
                        ORDER BY
                        T.NORD ASC"""
                data = self.fetch(sql, args={"dt": date}, as_dict=True, session=session)
                if len(data) > 0:
                    return float(data[0].get("base_amount", None))
        except Exception:
            logger.error(f"Error getting base value for date `{date}`", exc_info=True)
            return None

    def design(self):
        sql = """select
                    t.ID as "id", t.CODE as "code", t.PAY_ID as "pay_id", t.LONGNAME as "longname",
                    t.NOEMBFL as "noembfl", t.ADDCRDFL as "addcrdfl", b.BIN as "bin", s.CODE as "pay_code"
                from N_CRDTYPE t, N_CRDPAYSYS s, N_BIN b
                where t.PAY_ID = s.ID and  b.id = t.bin_id
                order by s.CODE, t.CODE"""
        data = self.fetch(sql, as_dict=True)
        return {item['bin'][:6]: item['pay_code'] for item in data}

cl = OracleClient()
import json

print(cl.client_by_client_code('120000665886'))
