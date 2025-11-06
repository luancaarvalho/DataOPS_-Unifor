"""
PyAirbyte Pipeline with Data Quality: GitHub → Bronze → Silver
================================================================
Este pipeline extrai dados do GitHub, valida qualidade e promove para Silver

Fluxo:
1. GitHub API → PyAirbyte extraction
2. Salvar em Bronze (raw data)
3. Executar Data Quality checks
4. Se DQ passar: Promover para Silver (validated data)
"""

import airbyte as ab
import os
import boto3
import gzip
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
from collections import Counter


class GitHubToMinIOPipeline:
    """Pipeline completo com Data Quality"""
    
    def __init__(self, github_token: str, repository: str):
        self.github_token = github_token
        self.repository = repository
        self.s3_client = self._init_s3_client()
        self.temp_dir = Path("/tmp/pyairbyte_export")
        self.temp_dir.mkdir(exist_ok=True)
        
        # Métricas do pipeline
        self.metrics = {
            'extraction': {},
            'data_quality': {},
            'promotion': {}
        }
    
    def _init_s3_client(self):
        """Inicializa cliente S3 para MinIO"""
        return boto3.client(
            's3',
            endpoint_url='http://localhost:9010',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1'
        )
    
    def extract_from_github(self, streams: List[str]) -> ab.ReadResult:
        """Extrai dados do GitHub usando PyAirbyte"""
        print("="*60)
        print("📥 FASE 1: EXTRAÇÃO DO GITHUB")
        print("="*60)
        
        # Configurar source
        print(f"\n📦 Repositório: {self.repository}")
        print(f"📋 Streams: {', '.join(streams)}")
        
        source = ab.get_source(
            "source-github",
            config={
                "credentials": {"personal_access_token": self.github_token},
                "repositories": [self.repository],
                "start_date": "2024-01-01T00:00:00Z",
            },
            install_if_missing=True,
        )
        
        # Verificar conexão
        source.check()
        print("✅ GitHub conectado!")
        
        # Selecionar streams
        source.select_streams(streams)
        
        # Extrair dados
        print("\n⏳ Extraindo dados...")
        result = source.read()
        
        # Coletar métricas
        for stream_name, records in result.streams.items():
            count = len(list(records))
            self.metrics['extraction'][stream_name] = count
            print(f"  • {stream_name}: {count} registros")
         
        return result
    
    def save_to_bronze(self, result: ab.ReadResult) -> Dict[str, str]:
        """Salva dados extraídos no bucket Bronze do MinIO"""
        print("\n" + "="*60)
        print("💾 FASE 2: SALVANDO EM BRONZE (RAW DATA)")
        print("="*60)
        
        # Verificar conexão
        try:
            self.s3_client.head_bucket(Bucket='bronze')
            print("✅ MinIO Bronze bucket acessível!")
        except Exception as e:
            print(f"❌ Erro ao acessar Bronze: {e}")
            raise
        
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        uploaded_files = {}
        
        for stream_name, records in result.streams.items():
            record_list = list(records)
            
            if len(record_list) == 0:
                print(f"  ⏭️  {stream_name}: vazio, pulando")
                continue
            
            # Preparar arquivo
            filename = f"{stream_name}_{timestamp}.jsonl.gz"
            local_path = self.temp_dir / filename
            s3_key = f"github_data/{stream_name}/{filename}"
            
            # Escrever JSONL comprimido
            with gzip.open(local_path, 'wt', encoding='utf-8') as f:
                for record in record_list:
                    # Converter para dict
                    if hasattr(record, '__dict__'):
                        record_dict = record.__dict__
                    elif hasattr(record, 'to_dict'):
                        record_dict = record.to_dict()
                    else:
                        record_dict = dict(record) if not isinstance(record, dict) else record
                    
                    # Estrutura Airbyte
                    airbyte_record = {
                        "_airbyte_raw_id": str(record_dict.get("_airbyte_raw_id", "")),
                        "_airbyte_extracted_at": str(record_dict.get("_airbyte_extracted_at", "")),
                        "_airbyte_data": record_dict
                    }
                    f.write(json.dumps(airbyte_record, default=str) + '\n')
            
            # Upload para Bronze
            try:
                self.s3_client.upload_file(str(local_path), 'bronze', s3_key)
                uploaded_files[stream_name] = s3_key
                print(f"  ✅ {stream_name}: {len(record_list)} registros → bronze/{s3_key}")
            except Exception as e:
                print(f"  ❌ Erro no upload de {stream_name}: {e}")
                raise
            
            # Manter arquivo local para DQ checks
        
        return uploaded_files
    
    def run_data_quality_checks(self, bronze_files: Dict[str, str]) -> Dict[str, Dict]:
        """Executa verificações de qualidade nos dados Bronze"""
        print("\n" + "="*60)
        print("🔍 FASE 3: DATA QUALITY CHECKS")
        print("="*60)
        
        dq_results = {}
        
        for stream_name, s3_key in bronze_files.items():
            print(f"\n📊 Verificando qualidade: {stream_name}")
            print("-" * 40)
            
            # Baixar arquivo do Bronze
            local_file = self.temp_dir / Path(s3_key).name
            
            # Carregar dados
            records = []
            with gzip.open(local_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    records.append(json.loads(line.strip()))
            
            # Executar checks
            errors = []
            warnings = []
            
            # 1. Schema compliance
            required_fields = ['_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_data']
            schema_issues = 0
            for idx, record in enumerate(records):
                missing = [f for f in required_fields if f not in record]
                if missing:
                    schema_issues += 1
                    errors.append({
                        'type': 'SCHEMA_VIOLATION',
                        'record_index': idx,
                        'missing_fields': missing
                    })
            
            if schema_issues == 0:
                print(f"  ✅ Schema: todos os {len(records)} registros válidos")
            else:
                print(f"  ❌ Schema: {schema_issues} violações")
            
            # 2. Duplicates
            ids = [r.get('_airbyte_raw_id') for r in records]
            id_counts = Counter(ids)
            duplicates = {rid: cnt for rid, cnt in id_counts.items() if cnt > 1}
            
            if not duplicates:
                print(f"  ✅ Duplicatas: nenhuma encontrada")
            else:
                print(f"  ❌ Duplicatas: {len(duplicates)} IDs duplicados")
                for rid, cnt in duplicates.items():
                    errors.append({
                        'type': 'DUPLICATE_ID',
                        'raw_id': rid,
                        'count': cnt
                    })
            
            # 3. Completeness
            total = len(records)
            null_counts = {}
            for record in records:
                data = record.get('_airbyte_data', {})
                for key, value in data.items():
                    if value is None or value == "":
                        null_counts[key] = null_counts.get(key, 0) + 1
            
            completeness = {}
            for field, null_count in null_counts.items():
                pct = ((total - null_count) / total) * 100 if total > 0 else 0
                completeness[field] = round(pct, 2)
                if pct < 80:
                    warnings.append({
                        'type': 'LOW_COMPLETENESS',
                        'field': field,
                        'completeness_pct': pct
                    })
            
            if null_counts:
                print(f"  📊 Completude: {len(null_counts)} campos com valores nulos")
            else:
                print(f"  ✅ Completude: 100% em todos os campos")
            
            # 4. Freshness
            now = datetime.now()
            ages = []
            for record in records:
                extracted_at_str = record.get('_airbyte_extracted_at', '')
                try:
                    extracted_at = datetime.fromisoformat(extracted_at_str.replace('Z', '+00:00'))
                    if extracted_at.tzinfo:
                        extracted_at = extracted_at.replace(tzinfo=None)
                    age_hours = (now - extracted_at).total_seconds() / 3600
                    ages.append(age_hours)
                except:
                    pass
            
            if ages:
                avg_age = sum(ages) / len(ages)
                print(f"  ✅ Freshness: idade média {round(avg_age, 2)}h")
            
            # Calcular quality score
            total_checks = len(records)
            total_errors = len(errors)
            quality_score = ((total_checks - total_errors) / total_checks * 100) if total_checks > 0 else 0
            
            dq_results[stream_name] = {
                'total_records': len(records),
                'errors': errors,
                'warnings': warnings,
                'quality_score': round(quality_score, 2),
                'completeness': completeness,
                'passed': len(errors) == 0
            }
            
            # Salvar relatório DQ
            report_key = f"dq_reports/{stream_name}/{Path(s3_key).stem}_report.json"
            report_data = {
                'timestamp': datetime.now().isoformat(),
                'stream': stream_name,
                'bronze_file': s3_key,
                'results': dq_results[stream_name]
            }
            
            report_local = self.temp_dir / f"{stream_name}_dq_report.json"
            with open(report_local, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            self.s3_client.upload_file(str(report_local), 'logs', report_key)
            report_local.unlink()
            
            print(f"  📄 Relatório DQ → logs/{report_key}")
            
            if dq_results[stream_name]['passed']:
                print(f"  ✅ Quality Score: {quality_score:.2f}/100 - APROVADO")
            else:
                print(f"  ❌ Quality Score: {quality_score:.2f}/100 - REPROVADO")
        
        self.metrics['data_quality'] = dq_results
        return dq_results
    
    def promote_to_silver(self, bronze_files: Dict[str, str], dq_results: Dict[str, Dict]) -> Dict[str, str]:
        """Promove dados validados de Bronze para Silver"""
        print("\n" + "="*60)
        print("🥈 FASE 4: PROMOÇÃO PARA SILVER (VALIDATED DATA)")
        print("="*60)
        
        silver_files = {}
        
        for stream_name, bronze_key in bronze_files.items():
            dq_result = dq_results.get(stream_name, {})
            
            if not dq_result.get('passed', False):
                print(f"  ⏭️  {stream_name}: DQ falhou, não promovendo para Silver")
                continue
            
            # Copiar de Bronze para Silver
            bronze_filename = Path(bronze_key).name
            silver_key = f"github_data/{stream_name}/{bronze_filename}"
            
            try:
                # Copiar objeto
                copy_source = {'Bucket': 'bronze', 'Key': bronze_key}
                self.s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket='silver',
                    Key=silver_key
                )
                
                silver_files[stream_name] = silver_key
                
                # Adicionar metadados de promoção
                metadata_key = f"github_data/{stream_name}/_metadata/{Path(bronze_filename).stem}.json"
                metadata = {
                    'promoted_at': datetime.now().isoformat(),
                    'source_bucket': 'bronze',
                    'source_key': bronze_key,
                    'quality_score': dq_result.get('quality_score'),
                    'record_count': dq_result.get('total_records'),
                    'promoted_by': 'pyairbyte_pipeline'
                }
                
                self.s3_client.put_object(
                    Bucket='silver',
                    Key=metadata_key,
                    Body=json.dumps(metadata, indent=2),
                    ContentType='application/json'
                )
                
                print(f"  ✅ {stream_name}: {dq_result['total_records']} registros → silver/{silver_key}")
                print(f"     Quality Score: {dq_result['quality_score']:.2f}/100")
                
            except Exception as e:
                print(f"  ❌ Erro ao promover {stream_name}: {e}")
        
        self.metrics['promotion'] = {
            'total_streams': len(bronze_files),
            'promoted_streams': len(silver_files),
            'promotion_rate': round(len(silver_files) / len(bronze_files) * 100, 2) if bronze_files else 0
        }
        
        return silver_files
    
    def cleanup(self):
        """Limpa arquivos temporários"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def print_summary(self):
        """Imprime resumo do pipeline"""
        print("\n" + "="*60)
        print("📊 RESUMO DO PIPELINE")
        print("="*60)
        
        # Extração
        print("\n📥 Extração:")
        total_extracted = sum(self.metrics['extraction'].values())
        print(f"  • Total de registros: {total_extracted}")
        for stream, count in self.metrics['extraction'].items():
            print(f"    - {stream}: {count}")
        
        # Data Quality
        print("\n🔍 Data Quality:")
        dq = self.metrics['data_quality']
        passed = sum(1 for r in dq.values() if r.get('passed', False))
        total = len(dq)
        print(f"  • Streams aprovados: {passed}/{total}")
        for stream, result in dq.items():
            status = "✅ PASS" if result.get('passed') else "❌ FAIL"
            score = result.get('quality_score', 0)
            print(f"    - {stream}: {status} (Score: {score:.2f}/100)")
        
        # Promoção
        print("\n🥈 Promoção para Silver:")
        promo = self.metrics['promotion']
        print(f"  • Taxa de promoção: {promo.get('promotion_rate', 0):.2f}%")
        print(f"  • Streams promovidos: {promo.get('promoted_streams', 0)}/{promo.get('total_streams', 0)}")
        
        print("\n" + "="*60)
        print("✅ Pipeline concluído!")
        print("="*60)
        print(f"\n🌐 MinIO Console: http://localhost:9011")
        print(f"📂 Bronze (raw): bronze/github_data/")
        print(f"📂 Silver (validated): silver/github_data/")
        print(f"📂 DQ Reports: logs/dq_reports/")
    
    def run(self, streams: List[str]):
        """Executa pipeline completo"""
        try:
            # 1. Extrair do GitHub
            result = self.extract_from_github(streams)
            
            # 2. Salvar em Bronze
            bronze_files = self.save_to_bronze(result)
            
            # 3. Data Quality Checks
            dq_results = self.run_data_quality_checks(bronze_files)
            
            # 4. Promover para Silver
            silver_files = self.promote_to_silver(bronze_files, dq_results)
            
            # 5. Resumo
            self.print_summary()
            
            return True
            
        except Exception as e:
            print(f"\n❌ Erro no pipeline: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            self.cleanup()


# Execução
if __name__ == "__main__":
    # Configurações
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "your_token_here")
    REPO = "luancaarvalho/DataOPS_-Unifor"
    STREAMS = ["issues", "pull_requests", "stargazers", "commits"]
    
    print("🚀 PyAirbyte Pipeline com Data Quality")
    print(f"📦 Repositório: {REPO}")
    print("="*60)
    
    # Criar e executar pipeline
    pipeline = GitHubToMinIOPipeline(
        github_token=GITHUB_TOKEN,
        repository=REPO
    )
    
    success = pipeline.run(STREAMS)
    
    if success:
        print("\n✨ Pipeline executado com sucesso!")
    else:
        print("\n❌ Pipeline falhou. Verifique os logs acima.")
        exit(1)
