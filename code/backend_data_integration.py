import logging
import random
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from clust.data import data_interface
from config import influxdb_client as db_client
from config import mongodb_client

logger = logging.getLogger(__name__)

# FUNC001 - 데이터베이스 도메인 목록 조회
def get_domain_list() -> List[str]:
    """
    데이터베이스에서 고유 도메인 목록 추출.
    
    Args:
        None
        
    Returns:
        List[str]: 버킷 이름에서 추출된 고유 도메인 접두사 목록
        
    Process:
        1. InfluxDB에서 모든 데이터베이스 목록 조회
        2. 각 버킷 이름에서 도메인 접두사 추출 (첫 번째 언더스코어 이전)
        3. 중복 제거 후 고유 도메인 목록 반환
    """
    domain_list = []
    db_list = db_client.get_db_list()
    
    if db_list:
        db_list.sort()
    
    for item in db_list:
        prefix = item.split('_')[0]
        domain_list.append(prefix)
        
    unique_domain_list = list(set(domain_list))
    return unique_domain_list

# FUNC002 - 도메인별 측정값 목록 조회
def get_measurement_by_domain(domain: str) -> List[List[str]]:
    """
    특정 도메인에 속하는 모든 버킷과 측정값 조회.
    
    Args:
        domain (str): 필터링할 도메인 이름
        
    Returns:
        List[List[str]]: [bucket_name, measurement1, measurement2, ...] 형태의 목록
        
    Process:
        1. 모든 데이터베이스 버킷 목록 조회
        2. 지정된 도메인에 속하는 버킷 필터링
        3. 각 버킷의 측정값 목록 조회 후 결합
    """
    db_list = db_client.get_db_list()
    result = []
    
    for bucket_name in db_list:
        bucket_domain = bucket_name.split('_')[0]
        if bucket_domain == domain:
            ms_list = db_client.measurement_list(bucket_name)
            bucket_data = [bucket_name] + ms_list
            result.append(bucket_data)
    
    return result

# FUNC003 - MongoDB 메타데이터 조회
def get_ms_meta(bk_name: str, ms_name: str) -> dict:
    """
    버킷과 측정값에 대한 MongoDB 메타데이터 조회.
    
    Args:
        bk_name (str): 'database_collection_division' 형식의 버킷 이름
        ms_name (str): 측정값 이름
        
    Returns:
        dict: 메타데이터 문서 또는 오류 정보
        
    Process:
        1. 버킷 이름을 database, collection, division으로 파싱
        2. MongoDB에서 해당 메타데이터 검색
        3. 첫 번째 매칭 문서 반환 또는 오류 응답
    """
    try:
        db_name, other_bk_name = bk_name.split('_', 1)
        collection_name, division = other_bk_name.rsplit('_', 1)
    except ValueError:
        return {
            "error": "버킷 이름 형식이 올바르지 않습니다. 형식: database_collection_division",
            "status_code": 400
        }
        
    search = {'table_name': ms_name, 'division3': division}
    mongodb_results = mongodb_client.get_document_by_json(db_name, collection_name, search)
    
    if not mongodb_results or len(mongodb_results) == 0:
        return {
            "error": f"버킷 {bk_name}과 측정값 {ms_name}에 대한 정보를 찾을 수 없습니다.",
            "status_code": 404
        }
        
    return {"data": mongodb_results[0]}

# FUNC004 - 데이터 통합 처리
def process_data_integration(data):
    """
    두 데이터셋의 통합 처리 수행.
    
    Args:
        data (dict): 통합 요청 데이터
            - base_data: 기준 데이터 정보 [measurement, bucket, tag_key, tag_value, features]
            - selected_data: 선택 데이터 정보 [measurement, bucket, tag_key, tag_value, features]
            - start_time: 시작 시간
            - end_time: 종료 시간
            - Period: 기준 기간
            
    Returns:
        dict: 통합 결과 및 통계 정보
        
    Process:
        1. 입력 데이터 검증 및 파싱
        2. 각 데이터셋의 유효한 feature 확인
        3. 시간 범위 기반 데이터 수집
        4. 통합 통계 계산 및 결과 반환
    """
    base_data = data.get('base_data', [])
    selected_data = data.get('selected_data', [])
    start_time = data.get('start_time', '')
    end_time = data.get('end_time', '')
    period = data.get('Period', 'data1')
    
    if not base_data or not selected_data:
        return {
            'status': 'error',
            'message': 'base_data 또는 selected_data 정보가 없습니다.'
        }
        
    def parse_data_info(data_array):
        if len(data_array) < 2:
            return None
        
        measurement = data_array[0]
        bucket = data_array[1]
        
        tags = {}
        if len(data_array) >= 4:
            tag_key = data_array[2]
            tag_value = data_array[3]
            
            if tag_key and tag_value and tag_key != '' and tag_value != '':
                # 'ALL' 값인 경우 태그 필터링을 하지 않음
                if tag_value != 'ALL':
                    tags = {tag_key: tag_value}
                # ALL인 경우 tags는 빈 딕셔너리로 두어 모든 태그 값을 포함
        
        feature_list = []
        if len(data_array) > 4 and isinstance(data_array[4], list):
            feature_list = data_array[4]
        
        return {
            'measurement': measurement,
            'bucket': bucket,
            'tags': tags,
            'features': feature_list
        }
    
    base_info = parse_data_info(base_data)
    selected_info = parse_data_info(selected_data)
    
    if not base_info or not selected_info:
        return {
            'status': 'error',
            'message': '데이터 형식이 올바르지 않습니다.'
        }
        
    def validate_features_exist(measurement, bucket, tags, requested_features):
        test_result = data_interface.get_data_result("ms_by_time", db_client, {
            'bucket_name': bucket,
            'ms_name': measurement,
            'start_time': f"{start_time} 00:00:00" if len(start_time) == 10 else start_time,
            'end_time': f"{start_time} 23:59:59" if len(start_time) == 10 else start_time,
            'tags': tags
        })
        
        if test_result is None or (hasattr(test_result, '__len__') and len(test_result) == 0):
            return []
        
        available_columns = []
        if hasattr(test_result, 'columns'):
            available_columns = list(test_result.columns)
        elif isinstance(test_result, dict) and test_result:
            first_value = next(iter(test_result.values()))
            if hasattr(first_value, 'columns'):
                available_columns = list(first_value.columns)
        
        valid_features = []
        for feature in requested_features:
            if feature in available_columns:
                valid_features.append(feature)
        
        return valid_features
    
    # 필드 검증을 생략하고 사용자가 요청한 필드를 그대로 사용
    # (버킷 존재 여부 검증에서 404 오류가 발생하므로 기존 동작 방식 유지)
    valid_base_features = base_info['features']
    valid_selected_features = selected_info['features']
    
    # 기존 검증 로직 주석 처리
    # valid_base_features = validate_features_exist(
    #     base_info['measurement'], 
    #     base_info['bucket'], 
    #     base_info['tags'], 
    #     base_info['features']
    # )
    # 
    # valid_selected_features = validate_features_exist(
    #     selected_info['measurement'], 
    #     selected_info['bucket'], 
    #     selected_info['tags'], 
    #     selected_info['features']
    # )
    
    all_valid_features = list(set(valid_base_features + valid_selected_features))
    final_features = all_valid_features if all_valid_features else None
    
    # ms_list_info 형식: [bucket, measurement, tags] - makeIntDataInfoSet 함수가 이 순서를 기대함
    ms_list_info = [
        [base_info['bucket'], base_info['measurement'], base_info['tags']],
        [selected_info['bucket'], selected_info['measurement'], selected_info['tags']]
    ]
    
    formatted_start_time = f"{start_time} 00:00:00" if len(start_time) == 10 else start_time
    formatted_end_time = f"{end_time} 23:59:59" if len(end_time) == 10 else end_time
    
    ingestion_param = {
        'ms_list_info': ms_list_info,
        'start_time': formatted_start_time,
        'end_time': formatted_end_time
    }
    
    if final_features:
        ingestion_param['feature_list'] = final_features
    
    try:
        multiple_dataset = data_interface.get_data_result(
            "multiple_ms_by_time", 
            db_client,
            ingestion_param
        )
        
        if multiple_dataset is None:
            return {
                'status': 'error',
                'message': '데이터 수집 결과가 None입니다.'
            }
        
    except KeyError as e:
        return {
            'status': 'error',
            'message': f'요청한 컬럼을 찾을 수 없습니다: {str(e)}. 사용 가능한 컬럼을 확인해주세요.'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'데이터 수집 중 오류: {str(e)}'
        }
    
    total_rows = 0
    dataset_counts = {}
    
    if isinstance(multiple_dataset, dict):
        for key, dataset in multiple_dataset.items():
            if hasattr(dataset, '__len__'):
                count = len(dataset)
                dataset_counts[key] = count
                total_rows += count
    else:
        if hasattr(multiple_dataset, '__len__'):
            total_rows = len(multiple_dataset)
            dataset_counts['dataset_1'] = total_rows
        
        ms_list_info = [
            [base_info['measurement'], base_info['bucket'], base_info['tags']],
            [selected_info['measurement'], selected_info['bucket'], selected_info['tags']]
        ]
        
        formatted_start_time = f"{start_time} 00:00:00" if len(start_time) == 10 else start_time
        formatted_end_time = f"{end_time} 23:59:59" if len(end_time) == 10 else end_time
        
        ingestion_param = {
            'ms_list_info': ms_list_info,
            'start_time': formatted_start_time,
            'end_time': formatted_end_time
        }
        
        if final_features:
            ingestion_param['feature_list'] = final_features
        
        logger.info(f"Final ingestion parameters: {ingestion_param}")
        
        try:
            logger.info("Starting actual data collection...")
            
            multiple_dataset = data_interface.get_data_result(
                "multiple_ms_by_time", 
                db_client,
                ingestion_param
            )
            
            logger.info(f"Data collection completed successfully")
            logger.info(f"Result type: {type(multiple_dataset)}")
            
            if multiple_dataset is None:
                return {
                    'status': 'error',
                    'message': '데이터 수집 결과가 None입니다.'
                }
            
            if isinstance(multiple_dataset, dict):
                logger.info(f"Number of datasets: {len(multiple_dataset)}")
                for key, dataset in multiple_dataset.items():
                    logger.info(f"Dataset '{key}': type={type(dataset)}, shape={getattr(dataset, 'shape', 'N/A')}")
            
        except KeyError as e:
            logger.error(f"KeyError during data collection: {str(e)}")
            return {
                'status': 'error',
                'message': f'요청한 컬럼을 찾을 수 없습니다: {str(e)}. 사용 가능한 컬럼을 확인해주세요.'
            }
        except Exception as e:
            logger.error(f"Error during data collection: {str(e)}")
            return {
                'status': 'error',
                'message': f'데이터 수집 중 오류: {str(e)}'
            }
        
        total_rows = 0
        dataset_counts = {}
        
        if isinstance(multiple_dataset, dict):
            for key, dataset in multiple_dataset.items():
                if hasattr(dataset, '__len__'):
                    count = len(dataset)
                    dataset_counts[key] = count
                    total_rows += count
        else:
            if hasattr(multiple_dataset, '__len__'):
                total_rows = len(multiple_dataset)
                dataset_counts['dataset_1'] = total_rows
        
        
    return {
        'status': 'success',
        'result': {
            'original_datasets': len(dataset_counts),
            'dataset_counts': dataset_counts,
            'total_original_rows': total_rows,
            'integrated_stats': dataset_counts,
            'integration_frequency': '240T',
            'time_range': {
                'start': formatted_start_time,
                'end': formatted_end_time
            },
            'base_data_info': base_info,
            'selected_data_info': selected_info,
            'requested_features': base_info['features'] + selected_info['features'],
            'valid_features': final_features or [],
            'common_features': list(set(valid_base_features).intersection(set(valid_selected_features))),
            'period': period,
            'processing_notes': [
                f"Successfully processed {len(dataset_counts)} datasets",
                f"Total rows: {total_rows}",
                f"Valid features used: {final_features or 'All available'}",
                f"Requested features: {len(base_info['features'] + selected_info['features'])}",
                f"Valid features: {len(final_features) if final_features else 'All'}"
            ]
        },
        'message': '데이터 통합이 완료되었습니다.'
    }
# API: POST /IODataIntegration/data_integration

# FUNC005 - 유사 데이터 목록 조회
def get_secondary_data_list(primary_data: Dict[str, Any], tag_key: str = None, tag_value: str = None) -> List[List]:
    """
    기준 데이터와 시간 범위가 겹치는 유사 데이터 목록 조회.
    
    Args:
        primary_data (Dict[str, Any]): 기준 데이터 정보
            - bucket: 버킷 이름
            - measurements: 측정값 목록
        tag_key (str, optional): 태그 키
        tag_value (str, optional): 태그 값
        
    Returns:
        List[List]: 유사 데이터 목록 [bucket, measurement, tags, score, spatial_sim, keyword_sim, overlap_info]
        
    Process:
        1. 기준 데이터의 메타데이터에서 유사도 정보 조회
        2. 각 유사 데이터의 시간 범위 계산
        3. 시간 겹침 정보 및 유사도 점수 포함하여 반환
    """
    bucket = primary_data.get('bucket', '')
    
    measurements_data = primary_data.get('measurements', [])
    if isinstance(measurements_data, str):
        measurements = [measurements_data]
    else:
        measurements = measurements_data
    
    primary_time_range = extract_time_range(primary_data)
    if not primary_time_range:
        logger.error("첫 번째 데이터의 시간 범위를 찾을 수 없습니다.")
        return []
    
    if not bucket or not measurements:
        logger.error("유효하지 않은 primary_data 형식입니다.")
        return []
    
    logger.info(f"두 번째 데이터 목록 조회 시작 - 기준: {bucket}, 측정값: {measurements[0] if measurements else '없음'}")
    logger.info(f"첫 번째 데이터 시간 범위: {primary_time_range['start']} ~ {primary_time_range['end']}")
    
    try:
        measurement = measurements[0] if measurements else ""
        
        db_name, other_bk_name = bucket.split('_', 1)
        collection_name, division = other_bk_name.rsplit('_', 1)
        
        search = {'table_name': measurement, 'division3': division}
        logger.debug(f"MongoDB query parameters: {db_name}, {other_bk_name}, {collection_name}, {division}")
        
        try:
            statistical_info = mongodb_client.get_document_by_json(db_name, collection_name, search)[0]
        except Exception as e:
            logger.error(f"MongoDB 조회 중 오류 발생: {str(e)}")
            return []
        
        secondary_list = []
        
        if 'statistical_info' in statistical_info and len(statistical_info['statistical_info']) > 0:
            stat_info = statistical_info['statistical_info'][0]
            
            if 'similarity' in stat_info and len(stat_info['similarity']) > 0:
                sim_method = stat_info['similarity'][0]
                
                if 'result' in sim_method:
                    total_candidates = len(sim_method['result'])
                    
                    for result in sim_method['result']:
                        if 'target' in result and 'score' in result:
                            target = result['target']
                            target_bucket = target.get('bucket', '')
                            target_measurement = target.get('measurement', '')
                            score = result['score']
                            target_time_range = get_target_time_range(target_bucket, target_measurement)
                            
                            overlap_info = {}
                            if primary_time_range and target_time_range:
                                overlap_info = calculate_time_overlap(primary_time_range, target_time_range)
                            else:
                                overlap_info = {
                                    'overlap_days': 0,
                                    'overlap_ratio': 0.0,
                                    'overlap_start': None,
                                    'overlap_end': None
                                }
                            
                            if isinstance(score, float):
                                formatted_score = f"{score:.2f}"
                            else:
                                formatted_score = str(score)
                            
                            spatial_similarity = evaluate_spatial_similarity(
                                bucket, target_bucket, measurement, target_measurement
                            )
                            keyword_similarity = evaluate_keyword_similarity(
                                bucket, target_bucket, measurement, target_measurement
                            )
                            
                            secondary_list.append([
                                target_bucket,
                                target_measurement,
                                target.get('tags', {}),
                                formatted_score,
                                spatial_similarity,
                                keyword_similarity,
                                overlap_info
                            ])
                    
                    
                    logger.info(f"전체 두 번째 데이터 목록: {total_candidates}개 항목 모두 반환")
                    
                    
        logger.info(f"모든 두 번째 데이터 목록 조회 완료: {len(secondary_list)}개 항목")
        return secondary_list
    
    except Exception as e:
        logger.error(f"두 번째 데이터 목록 조회 중 오류 발생: {str(e)}")
        return []
# API: GET /IODataIntegration/secondary_data_list

# FUNC006 - 전체 유사 데이터 목록 조회
def get_all_secondary_data_list(primary_data: Dict[str, Any], tag_key: str = None, tag_value: str = None) -> List[List]:
    """
    시간 겹침 필터링 없이 모든 유사 데이터 반환.
    
    Args:
        primary_data (Dict[str, Any]): 기준 데이터 정보
        tag_key (str, optional): 태그 키
        tag_value (str, optional): 태그 값
        
    Returns:
        List[List]: 모든 유사 데이터 목록
        
    Process:
        1. 기준 데이터의 메타데이터에서 유사도 정보 조회
        2. 시간 필터링 없이 모든 유사 데이터 반환
        3. 각 데이터의 유사도 점수 및 메타정보 포함
    """
    bucket = primary_data.get('bucket', '')
    
    measurements_data = primary_data.get('measurements', [])
    if isinstance(measurements_data, str):
        measurements = [measurements_data]
    else:
        measurements = measurements_data
    
    if not bucket or not measurements:
        logger.error("유효하지 않은 primary_data 형식입니다.")
        return []
    
    logger.info(f"모든 두 번째 데이터 목록 조회 시작 - 기준: {bucket}, 측정값: {measurements[0] if measurements else '없음'}")
    
    try:
        measurement = measurements[0] if measurements else ""
        
        db_name, other_bk_name = bucket.split('_', 1)
        collection_name, division = other_bk_name.rsplit('_', 1)
        
        search = {'table_name': measurement, 'division3': division}
        logger.debug(f"MongoDB query parameters: {db_name}, {other_bk_name}, {collection_name}, {division}")
        
        try:
            statistical_info = mongodb_client.get_document_by_json(db_name, collection_name, search)[0]
        except Exception as e:
            logger.error(f"MongoDB 조회 중 오류 발생: {str(e)}")
            return []
        
        secondary_list = []
        
        if 'statistical_info' in statistical_info and len(statistical_info['statistical_info']) > 0:
            stat_info = statistical_info['statistical_info'][0]
            
            if 'similarity' in stat_info and len(stat_info['similarity']) > 0:
                sim_method = stat_info['similarity'][0]
                
                if 'result' in sim_method:
                    total_candidates = len(sim_method['result'])
                    
                    for result in sim_method['result']:
                        if 'target' in result and 'score' in result:
                            target = result['target']
                            target_bucket = target.get('bucket', '')
                            target_measurement = target.get('measurement', '')
                            score = result['score']
                            
                            # 시간 범위 정보는 조회하지만 필터링하지 않음
                            target_time_range = get_target_time_range(target_bucket, target_measurement)
                            primary_time_range = extract_time_range(primary_data)
                            
                            overlap_info = {}
                            if primary_time_range and target_time_range:
                                overlap_info = calculate_time_overlap(primary_time_range, target_time_range)
                            else:
                                overlap_info = {
                                    'overlap_days': 0,
                                    'overlap_ratio': 0.0,
                                    'overlap_start': None,
                                    'overlap_end': None
                                }
                            
                            if isinstance(score, float):
                                formatted_score = f"{score:.2f}"
                            else:
                                formatted_score = str(score)
                            
                            spatial_similarity = evaluate_spatial_similarity(
                                bucket, target_bucket, measurement, target_measurement
                            )
                            keyword_similarity = evaluate_keyword_similarity(
                                bucket, target_bucket, measurement, target_measurement
                            )
                            
                            secondary_list.append([
                                target_bucket,
                                target_measurement,
                                target.get('tags', {}),
                                formatted_score,
                                spatial_similarity,
                                keyword_similarity,
                                overlap_info
                            ])
                    
                    logger.info(f"전체 두 번째 데이터 목록: {total_candidates}개 항목 모두 반환")
        
        logger.info(f"모든 두 번째 데이터 목록 조회 완료: {len(secondary_list)}개 항목")
        return secondary_list
    
    except Exception as e:
        logger.error(f"모든 두 번째 데이터 목록 조회 중 오류 발생: {str(e)}")
        return []
# API: GET /IODataIntegration/all_secondary_data_list

# FUNC007 - 데이터 시간 범위 추출
def extract_time_range(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    데이터 메타정보에서 시간 범위 추출.
    
    Args:
        data (Dict[str, Any]): 데이터 정보 (bucket, measurements 포함)
        
    Returns:
        Dict[str, Any]: 시간 범위 정보 {'start': datetime, 'end': datetime} 또는 None
        
    Process:
        1. 데이터의 버킷과 측정값 정보 추출
        2. 메타데이터에서 시간 범위 정보 조회
        3. startTime, endTime을 datetime 객체로 변환하여 반환
    """
    try:
        bucket = data.get('bucket', '')
        measurements = data.get('measurements', [])
        
        if isinstance(measurements, str):
            measurement = measurements
        elif isinstance(measurements, list) and len(measurements) > 0:
            measurement = measurements[0]
        else:
            logger.error("측정값 정보를 찾을 수 없습니다.")
            return None
        
        meta_result = get_ms_meta(bucket, measurement)
        
        if 'error' in meta_result:
            logger.error(f"메타데이터 조회 실패: {meta_result['error']}")
            return None
        
        meta_data = meta_result.get('data', {})
        
        if 'statistical_info' in meta_data and len(meta_data['statistical_info']) > 0:
            stat_info = meta_data['statistical_info'][0]
            
            if 'filtered_info' in stat_info:
                filtered_info = stat_info['filtered_info']
                start_time = filtered_info.get('startTime')
                end_time = filtered_info.get('endTime')
                
                if start_time and end_time:
                    return {
                        'start': parse_datetime(start_time),
                        'end': parse_datetime(end_time)
                    }
        
        logger.warning(f"메타데이터에서 시간 정보를 찾을 수 없습니다: {bucket}/{measurement}")
        return None
            
    except Exception as e:
        logger.error(f"시간 범위 추출 중 오류: {str(e)}")
        return None

# FUNC008 - 대상 데이터 시간 범위 조회
def get_target_time_range(bucket: str, measurement: str) -> Dict[str, Any]:
    """
    특정 버킷/측정값의 시간 범위 조회.
    
    Args:
        bucket (str): 버킷 이름
        measurement (str): 측정값 이름
        
    Returns:
        Dict[str, Any]: 시간 범위 정보 또는 None
        
    Process:
        1. 지정된 버킷/측정값의 메타데이터 조회
        2. filtered_info에서 시간 범위 정보 추출
        3. datetime 객체로 변환하여 반환
    """
    try:
        meta_result = get_ms_meta(bucket, measurement)
        
        if 'error' in meta_result:
            logger.error(f"대상 데이터 메타데이터 조회 실패: {meta_result['error']}")
            return None
        
        meta_data = meta_result.get('data', {})
        
        if 'statistical_info' in meta_data and len(meta_data['statistical_info']) > 0:
            stat_info = meta_data['statistical_info'][0]
            
            if 'filtered_info' in stat_info:
                filtered_info = stat_info['filtered_info']
                start_time = filtered_info.get('startTime')
                end_time = filtered_info.get('endTime')
                
                if start_time and end_time:
                    return {
                        'start': parse_datetime(start_time),
                        'end': parse_datetime(end_time)
                    }
        
        logger.warning(f"대상 데이터의 메타데이터에서 시간 정보를 찾을 수 없습니다: {bucket}/{measurement}")
        return None
        
    except Exception as e:
        logger.error(f"대상 데이터 시간 범위 조회 중 오류: {str(e)}")
        return None

# FUNC009 - 시간 범위 겹침 확인
def is_time_overlap(range1: Dict[str, Any], range2: Dict[str, Any]) -> bool:
    """
    두 시간 범위의 겹침 여부 확인.
    
    Args:
        range1 (Dict[str, Any]): 첫 번째 시간 범위
        range2 (Dict[str, Any]): 두 번째 시간 범위
        
    Returns:
        bool: 겹침 여부
        
    Process:
        1. 각 시간 범위를 날짜 단위로 변환
        2. 겹치는 구간이 있는지 확인
        3. boolean 결과 반환
    """
    try:
        if not range1 or not range2:
            return False
            
        start1, end1 = range1['start'], range1['end']
        start2, end2 = range2['start'], range2['end']
        
        start1_date = start1.date()
        end1_date = end1.date()
        start2_date = start2.date()
        end2_date = end2.date()
        
        if end1_date < start2_date or start1_date > end2_date:
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"시간 범위 겹침 확인 중 오류: {str(e)}")
        return False

# FUNC010 - 시간 겹침 상세 계산
def calculate_time_overlap(range1: Dict[str, Any], range2: Dict[str, Any]) -> Dict[str, Any]:
    """
    두 시간 범위의 겹치는 구간과 비율 계산.
    
    Args:
        range1 (Dict[str, Any]): 첫 번째 시간 범위
        range2 (Dict[str, Any]): 두 번째 시간 범위
        
    Returns:
        Dict[str, Any]: 겹침 정보 (시작일, 종료일, 일수, 비율 등)
        
    Process:
        1. 겹치는 시작일과 종료일 계산
        2. 겹치는 일수 계산
        3. 각 범위 대비 겹침 비율 계산
        4. 상세 겹침 정보 반환
    """
    try:
        start1, end1 = range1['start'], range1['end']
        start2, end2 = range2['start'], range2['end']
        
        start1_date = start1.date()
        end1_date = end1.date()
        start2_date = start2.date()
        end2_date = end2.date()
        
        overlap_start_date = max(start1_date, start2_date)
        overlap_end_date = min(end1_date, end2_date)
        
        overlap_days = (overlap_end_date - overlap_start_date).days + 1
        range1_days = (end1_date - start1_date).days + 1
        range2_days = (end2_date - start2_date).days + 1
        
        overlap_ratio1 = overlap_days / range1_days if range1_days > 0 else 0
        overlap_ratio2 = overlap_days / range2_days if range2_days > 0 else 0
        
        return {
            'overlap_start': overlap_start_date.strftime('%Y-%m-%d'),
            'overlap_end': overlap_end_date.strftime('%Y-%m-%d'),
            'overlap_duration_days': overlap_days,
            'overlap_ratio_primary': round(overlap_ratio1, 4),
            'overlap_ratio_target': round(overlap_ratio2, 4),
            'overlap_days': overlap_days
        }
        
    except Exception as e:
        logger.error(f"시간 범위 겹침 계산 중 오류: {str(e)}")
        return {}

# FUNC011 - 날짜/시간 파싱
def parse_datetime(datetime_input) -> datetime:
    """
    다양한 형태의 날짜/시간 입력을 datetime 객체로 변환.
    
    Args:
        datetime_input: 날짜/시간 입력 (문자열, 숫자, datetime 등)
        
    Returns:
        datetime: 변환된 datetime 객체
        
    Process:
        1. 입력 타입별 처리 (datetime, timestamp, 문자열)
        2. 다양한 날짜 형식 패턴 시도
        3. 변환 실패시 현재 시간 반환
    """
    try:
        if isinstance(datetime_input, datetime):
            return datetime_input.replace(tzinfo=None) if datetime_input.tzinfo else datetime_input
        
        if isinstance(datetime_input, (int, float)):
            if datetime_input > 1e10:
                return datetime.fromtimestamp(datetime_input / 1000)
            else:
                return datetime.fromtimestamp(datetime_input)
        
        if isinstance(datetime_input, str):
            try:
                timestamp = float(datetime_input)
                if timestamp > 1e10:
                    return datetime.fromtimestamp(timestamp / 1000)
                else:
                    return datetime.fromtimestamp(timestamp)
            except ValueError:
                pass
            
            if '+00:00' in datetime_input or 'Z' in datetime_input:
                clean_str = datetime_input.replace('+00:00', '').replace('Z', '')
                try:
                    return datetime.fromisoformat(clean_str)
                except ValueError:
                    pass
            
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%d',
                '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d',
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y',
                '%Y%m%d%H%M%S',
                '%Y%m%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(datetime_input, fmt)
                except ValueError:
                    continue
        
        logger.warning(f"날짜 파싱 실패, 현재 시간 사용: {datetime_input}")
        return datetime.now()
        
    except Exception as e:
        logger.error(f"날짜 파싱 중 오류: {str(e)}")
        return datetime.now()

# FUNC012 - 공간 유사도 평가
def evaluate_spatial_similarity(source_bucket, target_bucket, source_measurement, target_measurement):
    """
    두 데이터셋의 공간적 유사성 평가.
    
    Args:
        source_bucket (str): 원본 버킷 이름
        target_bucket (str): 대상 버킷 이름
        source_measurement (str): 원본 측정값
        target_measurement (str): 대상 측정값
        
    Returns:
        bool: 공간 유사도 (현재 임시 랜덤값)
        
    Process:
        1. 위치 정보 기반 유사도 계산
        2. 현재는 임시로 랜덤값 반환
    """
    return random.choice([True, False])

# FUNC013 - 키워드 유사도 평가
def evaluate_keyword_similarity(source_bucket, target_bucket, source_measurement, target_measurement):
    """
    두 데이터셋의 키워드/메타데이터 유사성 평가.
    
    Args:
        source_bucket (str): 원본 버킷 이름
        target_bucket (str): 대상 버킷 이름
        source_measurement (str): 원본 측정값
        target_measurement (str): 대상 측정값
        
    Returns:
        bool: 키워드 유사도 (현재 임시 랜덤값)
        
    Process:
        1. 메타데이터 키워드 기반 유사도 계산
        2. 현재는 임시로 랜덤값 반환
    """
    return random.choice([True, False])

# FUNC014 - 데이터 필드 목록 조회
def get_fields(bucket: str, measurement: str, tag_key: str = None, tag_value: str = None) -> List[str]:
    """
    버킷과 측정값에 해당하는 컬럼 목록 조회.
    
    Args:
        bucket (str): 버킷 이름
        measurement (str): 측정값 이름
        tag_key (str, optional): 태그 키
        tag_value (str, optional): 태그 값
        
    Returns:
        List[str]: 컬럼 목록
        
    Process:
        1. 메타데이터에서 컬럼 정보 조회
        2. filtered_info의 columns 필드에서 컬럼 목록 추출
        3. 컬럼 목록 반환
    """
    try:
        db_name, other_bk_name = bucket.split('_', 1)
        collection_name, division = other_bk_name.rsplit('_', 1)
        
        search = {'table_name': measurement, 'division3': division}
        
        try:
            doc = mongodb_client.get_document_by_json(db_name, collection_name, search)[0]
        except Exception as e:
            logger.error(f"필드 목록 조회 중 MongoDB 오류 발생: {str(e)}")
            return []
        
        fields = []
        
        if 'statistical_info' in doc and len(doc['statistical_info']) > 0:
            stat_info = doc['statistical_info'][0]
            
            if 'filtered_info' in stat_info and 'columns' in stat_info['filtered_info']:
                fields = stat_info['filtered_info']['columns']
        
        logger.info(f"필드 목록 조회 완료: {bucket}/{measurement} - {len(fields)}개 필드")
        return fields
    
    except Exception as e:
        logger.error(f"필드 목록 조회 중 오류 발생: {str(e)}")
        return []
# API: POST /IODataIntegration/field_list

# FUNC015 - 태그 값 목록 조회
def get_tag_values(bucket: str, measurement: str, tag_key: str) -> List[str]:
    """
    특정 버킷/측정값/태그키에 대한 태그 값 목록 조회.
    
    Args:
        bucket (str): 버킷 이름
        measurement (str): 측정값 이름  
        tag_key (str): 태그 키
        
    Returns:
        List[str]: 태그 값 목록 (ALL 옵션 포함)
        
    Process:
        1. InfluxDB에서 특정 태그키의 고유 값들 조회
        2. 목록 상단에 'ALL' 옵션 추가
        3. 정렬된 태그 값 목록 반환
    """
    try:
        logger.info(f"태그 값 조회 시작: {bucket}/{measurement}/{tag_key}")
        
        # InfluxDB에서 태그 값 목록 조회
        tag_values = db_client.get_tag_values(bucket, measurement, tag_key)
        
        if not tag_values:
            logger.warning(f"태그 값이 없습니다: {bucket}/{measurement}/{tag_key}")
            return ['ALL']  # 값이 없어도 ALL 옵션은 제공
        
        # 중복 제거 및 정렬
        unique_values = sorted(list(set(tag_values)))
        
        # ALL 옵션을 맨 앞에 추가
        result = ['ALL'] + unique_values
        
        logger.info(f"태그 값 조회 완료: {len(result)}개 (ALL 포함)")
        return result
        
    except Exception as e:
        logger.error(f"태그 값 조회 중 오류: {str(e)}")
        return ['ALL']  # 오류 시에도 ALL 옵션은 제공
# API: GET /IODataIntegration/tag_values

# FUNC016 - 통합 데이터 저장
def save_integration_data(request_data: Dict[str, Any]) -> str:
    """
    통합 데이터를 InfluxDB와 MongoDB에 저장.
    
    Args:
        request_data (Dict[str, Any]): 저장 요청 데이터
            - data1: 첫 번째 데이터 정보
            - data2: 두 번째 데이터 정보
            - selected_fields: 선택된 필드 목록
            - integration_result: 통합 결과
            
    Returns:
        str: 저장 ID 또는 오류 코드
        
    Process:
        1. 요청 데이터 검증 및 파싱
        2. 데이터 재통합 수행
        3. InfluxDB에 통합 데이터 저장
        4. MongoDB에 메타데이터 저장
        5. 저장 ID 반환
    """
    from clust.meta import generation_auto_ms_meta as gamm
    
    logger.info("통합 데이터 저장 프로세스 시작")
    
    try:
        if not request_data:
            logger.error("요청 데이터가 없습니다")
            return "error_no_data"
        
        logger.info(f"요청 데이터 수신: {list(request_data.keys())}")
        
        # 필수 필드 검증
        required_fields = ['data1', 'data2', 'selected_fields', 'integration_result']
        missing_fields = [field for field in required_fields if field not in request_data]
        
        if missing_fields:
            logger.error(f"필수 필드 누락: {missing_fields}")
            return f"error_missing_fields_{','.join(missing_fields)}"
        
        # 요청 데이터 추출
        data1 = request_data['data1']
        data2 = request_data['data2']
        selected_fields = request_data['selected_fields']
        integration_result = request_data['integration_result']
        
        # 저장 시 데이터 재통합 실행 - process_data_integration과 동일한 로직
        logger.info("저장을 위한 데이터 재통합 시작")
        
        try:
            # UI 데이터 구조를 process_data_integration 형식으로 변환
            integration_request = {
                'base_data': [data1.get('bucket'), data1.get('measurement'), data1.get('tagKey'), data1.get('tagValue'), selected_fields],
                'selected_data': [data2.get('bucket'), data2.get('measurement'), data2.get('tagKey'), data2.get('tagValue'), selected_fields],
                'start_time': integration_result.get('result', {}).get('time_range', {}).get('start', '2023-08-15'),
                'end_time': integration_result.get('result', {}).get('time_range', {}).get('end', '2023-08-15'),
                'Period': 'data1'
            }
            
            logger.info(f"재통합 요청 데이터: {integration_request}")
            
            # process_data_integration 함수 직접 호출하여 새로운 통합 수행
            fresh_integration_result = process_data_integration(integration_request)
            
            if fresh_integration_result.get('status') != 'success':
                logger.error(f"데이터 재통합 실패: {fresh_integration_result.get('message')}")
                return "error_reintegration_failed"
            
            logger.info("데이터 재통합 성공")
            
            # 재통합 과정에서 생성된 multiple_dataset을 직접 다시 조회
            # 태그 정보 구성 (ALL 값 처리 포함)
            data1_tags = {}
            if data1.get('tagKey') and data1.get('tagValue') and data1.get('tagValue') != 'ALL':
                data1_tags = {data1.get('tagKey'): data1.get('tagValue')}
                
            data2_tags = {}
            if data2.get('tagKey') and data2.get('tagValue') and data2.get('tagValue') != 'ALL':
                data2_tags = {data2.get('tagKey'): data2.get('tagValue')}
            
            ms_list_info = [
                [data1.get('bucket'), data1.get('measurement'), data1_tags],
                [data2.get('bucket'), data2.get('measurement'), data2_tags]
            ]
            
            # 시간 형식 정리 (중복 방지)
            start_time = integration_request['start_time']
            end_time = integration_request['end_time']
            
            # 이미 시간이 포함되어 있으면 그대로 사용, 아니면 시간 추가
            if ' ' not in start_time:
                start_time += ' 00:00:00'
            if ' ' not in end_time:
                end_time += ' 23:59:59'
            
            ingestion_param = {
                'ms_list_info': ms_list_info,
                'start_time': start_time,
                'end_time': end_time
            }
            
            if selected_fields:
                ingestion_param['feature_list'] = selected_fields
                
            logger.info(f"최종 데이터 조회 파라미터: {ingestion_param}")
            
            # feature_list가 있으면 제거하여 전체 데이터를 먼저 가져온 후 필터링
            ingestion_param_without_features = ingestion_param.copy()
            if 'feature_list' in ingestion_param_without_features:
                selected_features = ingestion_param_without_features.pop('feature_list')
                logger.info(f"Feature list 임시 제거하여 전체 데이터 조회: {selected_features}")
            else:
                selected_features = None
            
            # 실제 통합 데이터 조회
            multiple_dataset = data_interface.get_data_result(
                "multiple_ms_by_time", 
                db_client,
                ingestion_param_without_features
            )
            
            if multiple_dataset is None:
                logger.error("통합 데이터 조회 실패")
                return "error_no_integrated_data"
            
            # DataFrame 추출
            if isinstance(multiple_dataset, dict) and len(multiple_dataset) > 0:
                first_key = next(iter(multiple_dataset))
                integrated_data = multiple_dataset[first_key]
                logger.info(f"통합 데이터 추출 성공: {type(integrated_data)}, shape: {getattr(integrated_data, 'shape', 'N/A')}")
            else:
                integrated_data = multiple_dataset
                logger.info(f"통합 데이터 직접 사용: {type(integrated_data)}")
            
            # DataFrame으로 변환 후 필요시 컬럼 필터링
            if isinstance(integrated_data, pd.Series):
                integrated_data = integrated_data.to_frame().T
                logger.info(f"Series를 DataFrame으로 변환: {integrated_data.shape}")
            
            # 선택된 feature들만 필터링
            if selected_features and isinstance(integrated_data, pd.DataFrame):
                available_features = [col for col in selected_features if col in integrated_data.columns]
                if available_features:
                    integrated_data = integrated_data[available_features]
                    logger.info(f"Feature 필터링 적용: {available_features}, 결과 shape: {integrated_data.shape}")
                else:
                    logger.warning("선택된 feature가 데이터에 없어서 전체 데이터 사용")
                
        except Exception as e:
            logger.error(f"데이터 재통합 중 오류: {str(e)}")
            return f"error_reintegration_{str(e)[:50]}"
        
        # 기준 데이터 정보로 저장 위치 결정 (data1 기준)
        base_info = {
            'measurement': data1.get('measurement'),
            'bucket': data1.get('bucket'),
            'tags': {data1.get('tagKey'): data1.get('tagValue')} if data1.get('tagKey') and data1.get('tagValue') else {}
        }
        
        bk_name = base_info['bucket']
        ms_name = base_info['measurement']
        
        logger.info(f"기준 데이터 - 버킷: {bk_name}, 측정값: {ms_name}")
        
        # DataFrame 변환
        if isinstance(integrated_data, pd.DataFrame):
            selected_df = integrated_data.copy()
            logger.info(f"DataFrame 직접 사용: {selected_df.shape}")
        elif isinstance(integrated_data, pd.Series):
            # Series를 DataFrame으로 변환
            selected_df = integrated_data.to_frame().T
            logger.info(f"Series를 DataFrame으로 변환: {selected_df.shape}")
        elif isinstance(integrated_data, list):
            try:
                selected_df = pd.DataFrame(integrated_data)
                logger.info(f"List를 DataFrame으로 변환: {selected_df.shape}")
            except Exception as e:
                logger.error(f"DataFrame 변환 실패: {e}")
                return f"error_dataframe_conversion_{str(e)[:50]}"
        else:
            logger.error(f"지원되지 않는 데이터 타입: {type(integrated_data)}")
            return f"error_unsupported_type_{type(integrated_data)}"
        
        if selected_df.empty:
            logger.error("저장할 데이터가 비어있습니다")
            return "error_empty_dataframe"
        
        logger.info(f"통합 데이터 프레임 크기: {selected_df.shape}")
        
        # 태그 컬럼과 수치형 컬럼 구분
        tags = [col for col in selected_df.columns if selected_df[col].dtype == 'object']
        numeric_cols = [col for col in selected_df.columns if col not in tags and col != 'time']
        
        logger.info(f"태그 컬럼: {tags}")
        logger.info(f"수치형 컬럼: {numeric_cols}")
        
        # 수치형 컬럼 타입 변환
        for col in numeric_cols:
            try:
                selected_df[col] = pd.to_numeric(selected_df[col], errors='coerce').astype(float)
            except Exception as e:
                logger.warning(f"컬럼 '{col}' 타입 변환 실패: {e}")
        
        # 새로운 측정값 이름 생성 (_integration 접미사 추가)
        new_ms_name = f"{ms_name}_integration"
        logger.info(f"새로운 측정값 이름: {new_ms_name}")
        
        # 버킷 이름 파싱
        try:
            if '_' in bk_name:
                parts = bk_name.split('_')
                if len(parts) >= 3:
                    db_name = parts[0]
                    division = parts[-1]
                    collection_name = '_'.join(parts[1:-1])
                else:
                    db_name = bk_name
                    collection_name = "default"
                    division = "default"
            else:
                db_name = bk_name
                collection_name = "default"
                division = "default"
                
            logger.info(f"MongoDB 저장 정보 - DB: {db_name}, Collection: {collection_name}, Division: {division}")
        except Exception as e:
            logger.error(f"버킷 이름 파싱 실패: {e}")
            return f"error_bucket_parsing_{str(e)[:50]}"
        
        # 고유 저장 ID 생성
        save_id = f"integrated_{bk_name}_{new_ms_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"저장 ID 생성: {save_id}")
        
        # InfluxDB에 데이터 저장
        logger.info("InfluxDB에 통합 데이터 저장을 시작합니다")
        try:
            if tags and len(tags) > 0:
                logger.info(f"태그와 함께 저장: {tags}")
                db_client.write_db(bk_name, new_ms_name, selected_df, tags)
            else:
                logger.info("태그 없이 저장")
                db_client.write_db(bk_name, new_ms_name, selected_df)
            
            logger.info("InfluxDB 저장 성공")
        except Exception as e:
            logger.error(f"InfluxDB 저장 실패: {e}")
            return f"error_influxdb_save_{str(e)[:50]}"
        
        # MongoDB 메타데이터 처리
        logger.info("MongoDB 메타데이터 처리를 시작합니다")
        try:
            search_criteria = {'table_name': new_ms_name, 'division3': division}
            existing_doc = mongodb_client.get_document_by_json(db_name, collection_name, search_criteria)
            
            # 메타데이터 생성
            try:
                auto_ms_meta = gamm.get_auto_ms_meta(db_client, bk_name, new_ms_name, None)
                logger.info("gamm 모듈을 사용하여 메타데이터 생성")
            except:
                logger.warning("gamm 모듈을 사용할 수 없습니다. 기본 메타데이터를 생성합니다")
                auto_ms_meta = {
                    'table_name': new_ms_name,
                    'division3': division,
                    'bucket_name': bk_name,
                    'data_type': 'integration',
                    'original_measurement': ms_name,
                    'tag_keys': tags,
                    'numeric_columns': numeric_cols,
                    'created_at': datetime.now().isoformat(),
                    'save_id': save_id
                }
            
            # 통합 정보 추가
            auto_ms_meta.update({
                'data_type': 'integration',
                'integration_info': {
                    'data1': data1,
                    'data2': data2,
                    'selected_fields': selected_fields,
                    'integration_period': integration_result.get('result', {}).get('time_range', {})
                },
                'original_measurement': ms_name,
                'integrated_at': datetime.now().isoformat(),
                'save_id': save_id,
                'data_shape': list(selected_df.shape),
                'column_info': {
                    'tags': tags,
                    'numeric_columns': numeric_cols,
                    'total_columns': len(selected_df.columns)
                }
            })
            
            # MongoDB에 저장/업데이트
            if not existing_doc:
                logger.info("새로운 메타데이터 문서 삽입")
                mongodb_client.insert_document(db_name, collection_name, auto_ms_meta)
            else:
                logger.info("기존 메타데이터 문서 업데이트")
                mongodb_client.update_one_document(db_name, collection_name, search_criteria, auto_ms_meta)
            
            logger.info("MongoDB 메타데이터 저장 성공")
            
        except Exception as e:
            logger.warning(f"MongoDB 메타데이터 저장 실패 (데이터는 성공적으로 저장됨): {e}")
        
        logger.info(f"통합 데이터 저장 프로세스 완료 - 저장 ID: {save_id}")
        return save_id
        
    except Exception as e:  
        logger.error(f"통합 데이터 저장 중 오류 발생: {str(e)}")
        import traceback
        logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
        return f"error_unexpected_{str(e)[:50]}"
# API: POST /IODataIntegration/save_result

# FUNC017 - 통합 데이터 CSV 저장
def save_integration_csv(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    통합 데이터를 CSV 파일로 저장.
    
    Args:
        request_data (Dict[str, Any]): CSV 저장 요청 데이터
        
    Returns:
        Dict[str, Any]: 저장 결과 (상태, 파일 경로, 메타정보 등)
        
    Process:
        1. 요청 데이터 검증
        2. 통합 데이터 재생성
        3. DataFrame을 CSV 파일로 저장
        4. 저장 결과 및 파일 정보 반환
    """
    logger.info("통합 데이터 CSV 저장 프로세스 시작")
    
    try:
        if not request_data:
            return {
                'status': 'error',
                'message': '요청 데이터가 없습니다.'
            }
        
        # 필수 필드 확인
        required_fields = ['data1', 'data2', 'selected_fields', 'integration_result']
        for field in required_fields:
            if field not in request_data:
                return {
                    'status': 'error', 
                    'message': f'필수 필드가 누락되었습니다: {field}'
                }
        
        data1 = request_data['data1']
        data2 = request_data['data2']
        selected_fields = request_data['selected_fields']
        integration_result = request_data['integration_result']
        
        logger.info(f"CSV 저장 요청 - data1: {data1.get('bucket')}/{data1.get('measurement')}")
        logger.info(f"CSV 저장 요청 - data2: {data2.get('bucket')}/{data2.get('measurement')}")
        
        # 통합 데이터 재생성
        try:
            start_time = integration_result.get('result', {}).get('time_range', {}).get('start', '2023-08-15')
            end_time = integration_result.get('result', {}).get('time_range', {}).get('end', '2023-08-15')
            
            # 시간 문자열 처리 (중복 제거)
            if ' 00:00:00' in start_time and start_time.count('00:00:00') > 1:
                start_time = start_time.split(' 00:00:00')[0] + ' 00:00:00'
            if ' 00:00:00' in end_time and end_time.count('00:00:00') > 1:
                end_time = end_time.split(' 00:00:00')[0] + ' 00:00:00'
            
            integration_request = {
                'data1': data1,
                'data2': data2,
                'selected_fields': selected_fields,
                'start_time': start_time,
                'end_time': end_time,
                'Period': 'data1'
            }
            
            logger.info(f"CSV 저장용 재통합 요청: {integration_request}")
            
            # 데이터 재통합
            fresh_integration_result = process_data_integration(integration_request)
            
            if fresh_integration_result.get('status') != 'success':
                return {
                    'status': 'error',
                    'message': f"데이터 재통합 실패: {fresh_integration_result.get('message')}"
                }
            
            # 통합 데이터 추출
            result_data = fresh_integration_result.get('result', {})
            if 'integrated_data' in result_data:
                integrated_data = result_data['integrated_data']
            else:
                return {
                    'status': 'error',
                    'message': '통합 데이터를 찾을 수 없습니다.'
                }
            
            logger.info(f"재통합 데이터 타입: {type(integrated_data)}")
            
            # DataFrame 변환
            if isinstance(integrated_data, pd.Series):
                df_to_save = integrated_data.to_frame().T
            elif isinstance(integrated_data, pd.DataFrame):
                df_to_save = integrated_data.copy()
            else:
                return {
                    'status': 'error',
                    'message': f'지원되지 않는 데이터 타입: {type(integrated_data)}'
                }
            
            if df_to_save.empty:
                return {
                    'status': 'error',
                    'message': '저장할 데이터가 비어있습니다.'
                }
            
            logger.info(f"CSV 저장할 데이터 크기: {df_to_save.shape}")
            
            # CSV 파일 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"integration_{data1.get('measurement', 'unknown')}_{data2.get('measurement', 'unknown')}_{timestamp}.csv"
            
            # integration_results 폴더에 저장
            import os
            os.makedirs('integration_results', exist_ok=True)
            file_path = os.path.join('integration_results', filename)
            
            # CSV 저장
            df_to_save.to_csv(file_path, index=True, encoding='utf-8')
            
            logger.info(f"CSV 파일 저장 완료: {file_path}")
            
            return {
                'status': 'success',
                'message': 'CSV 파일 저장 성공',
                'file_path': file_path,
                'filename': filename,
                'data_shape': list(df_to_save.shape),
                'columns': list(df_to_save.columns),
                'timestamp': timestamp
            }
            
        except Exception as e:
            logger.error(f"데이터 재통합 중 오류: {str(e)}")
            return {
                'status': 'error',
                'message': f'데이터 재통합 실패: {str(e)}'
            }
        
    except Exception as e:
        logger.error(f"CSV 저장 중 오류 발생: {str(e)}")
        import traceback
        logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
        return {
            'status': 'error',
            'message': f'CSV 저장 실패: {str(e)}'
        }
# API: POST /IODataIntegration/save_csv