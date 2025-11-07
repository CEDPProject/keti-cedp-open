import uuid
from datetime import datetime

from flask import render_template, make_response, request, jsonify
from flask_restx import Namespace, Resource

from backend.backend_data_integration import (
    process_data_integration, save_integration_data, save_integration_csv,
    get_measurement_by_domain,
    get_secondary_data_list, get_all_secondary_data_list, get_fields, get_tag_values
)
from backend.backend_io_processing import (
    get_domain_list,
)

IODataIntegration = Namespace('IODataIntegration', description='Data Integration API')

html_headers = {
    "Content-Type": "text/html; charset=utf-8"
}

# ROUTE001 - 데이터 통합 메인 페이지
@IODataIntegration.route('/data_integration')
class Integration(Resource):
    def get(self):
        """
        도메인 목록과 함께 데이터 통합 메인 페이지 렌더링.
        
        Args:
            None
            
        Returns:
            Response: 도메인 목록이 포함된 HTML 페이지
            
        Process:
            1. 시스템에서 사용 가능한 모든 도메인 조회
            2. integration.html 템플릿에 도메인 목록 전달
            3. HTML 응답 렌더링 및 반환
        """
        domain_list = get_domain_list()
        return make_response(
            render_template('integration.html', domain_list=domain_list),
            200,
            html_headers
        )

    def post(self):
        """
        데이터 통합 요청 처리.
        
        Args:
            Request Body JSON:
                base_data (list): 기준 데이터 정보 [measurement, bucket, tag_key, tag_value, features]
                selected_data (list): 선택 데이터 정보 [measurement, bucket, tag_key, tag_value, features]
                start_time (str): 시작 시간
                end_time (str): 종료 시간
                Period (str): 기준 기간 ('data1' 또는 'data2')
                
        Returns:
            JSON: 통합 결과 및 통계 정보
            
        Process:
            1. 요청 데이터 검증
            2. 데이터 통합 처리 수행
            3. 결과에 타임스탬프와 요청 ID 추가
            4. 통합 결과 요약 생성 후 반환
        """
        request_data = request.get_json() if request.is_json else {}
        
        validation_result = validate_integration_request(request_data)
        if not validation_result['is_valid']:
            return jsonify({
                'status': 'error',
                'message': validation_result['message']
            }), 400
        
        result = process_data_integration(request_data)
        
        if result.get('status') == 'success':
            result['timestamp'] = datetime.now().isoformat()
            result['request_id'] = str(uuid.uuid4())
            
            if 'result' in result:
                result['summary'] = create_result_summary(result['result'], request_data)
        
        return jsonify(result)

# ROUTE002 - 통합 결과 데이터베이스 저장
@IODataIntegration.route('/save_result')
class SaveIntegrationResult(Resource):
    def post(self):
        """
        통합 결과를 데이터베이스에 저장.
        
        Args:
            Request Body JSON:
                data1 (dict): 첫 번째 데이터 정보
                data2 (dict): 두 번째 데이터 정보
                selected_fields (list): 선택된 필드 목록
                integration_result (dict): 통합 결과
                
        Returns:
            JSON: 저장 상태 및 저장 ID
            
        Process:
            1. 저장 데이터 검증
            2. InfluxDB와 MongoDB에 데이터 저장
            3. 저장 결과 및 ID 반환
        """
        save_data = request.get_json()
        
        if not save_data:
            return jsonify({
                'status': 'error',
                'message': '저장할 데이터가 없습니다.'
            }), 400
        
        save_id = save_integration_data(save_data)
        
        if save_id.startswith('error_'):
            return jsonify({
                'status': 'error',
                'message': f'저장 중 오류가 발생했습니다: {save_id}',
                'error_code': save_id
            }), 500
        
        return jsonify({
            'status': 'success',
            'message': '데이터가 성공적으로 DB에 저장되었습니다.',
            'save_id': save_id,
            'timestamp': datetime.now().isoformat()
        })

# ROUTE003 - 통합 결과 CSV 파일 저장
@IODataIntegration.route('/save_csv')
class SaveIntegrationCSV(Resource):
    def post(self):
        """
        통합 결과를 CSV 파일로 저장.
        
        Args:
            Request Body JSON: 통합 데이터 정보
            
        Returns:
            JSON: CSV 저장 결과 및 파일 정보
            
        Process:
            1. 저장 데이터 검증
            2. 통합 데이터 CSV 파일로 저장
            3. 파일 경로 및 메타정보 반환
        """
        save_data = request.get_json()
        
        if not save_data:
            return jsonify({
                'status': 'error',
                'message': '저장할 데이터가 없습니다.'
            }), 400
        
        result = save_integration_csv(save_data)
        
        if result.get('status') == 'success':
            return jsonify(result)
        else:
            return jsonify(result), 400

# ROUTE004 - 통합용 도메인 데이터 목록 조회
@IODataIntegration.route('/integratinon_data_list', endpoint='integratinon_data_list')
class Datalist(Resource):
    def get(self):
        """
        특정 도메인의 통합용 데이터 목록 조회.
        
        Args:
            domain (str): 조회할 도메인 이름
            
        Returns:
            JSON: 도메인에 속하는 버킷과 측정값 목록
            
        Process:
            1. 도메인 매개변수 검증
            2. 도메인에 따른 버킷 및 측정값 목록 조회
            3. 구조화된 JSON 응답 반환
        """
        domain = request.args.get('domain')
        
        if not domain:
            return jsonify({
                "status": "error",
                "message": "도메인 파라미터가 필요합니다",
                "data_list": []
            }), 400
        
        bucket_list = get_measurement_by_domain(domain)
        
        return jsonify({
            "status": "success",
            "domain": domain,
            "data_list": bucket_list
        })

# ROUTE005 - 두 번째 데이터 목록 조회 (시간 필터링)
@IODataIntegration.route('/secondary_data_list')
class SecondaryDataList(Resource):
    def get(self):
        """
        첫 번째 데이터와 시간 범위가 겹치는 두 번째 데이터 목록 조회.
        
        Args:
            bucket (str): 기준 버킷 이름
            measurement (str): 기준 측정값 이름
            tag_key (str, optional): 태그 키
            tag_value (str, optional): 태그 값
            
        Returns:
            JSON: 시간 범위가 겹치는 유사 데이터 목록
            
        Process:
            1. 필수 매개변수 검증
            2. 기준 데이터 정보 구성
            3. 시간 범위 겹침 기준 유사 데이터 조회
            4. 유사도 점수 및 겹침 정보와 함께 반환
        """
        bucket = request.args.get('bucket')
        measurement = request.args.get('measurement')
        tag_key = request.args.get('tag_key')
        tag_value = request.args.get('tag_value')
        
        if not bucket:
            return jsonify({
                "status": "error",
                "message": "버킷 정보가 필요합니다",
                "data_list": []
            }), 400
            
        if not measurement:
            return jsonify({
                "status": "error",
                "message": "측정값 정보가 필요합니다",
                "data_list": []
            }), 400
        
        primary_data = {
            'bucket': bucket,
            'measurements': measurement
        }
        secondary_list = get_secondary_data_list(primary_data, tag_key, tag_value)
        
        return jsonify({
            "status": "success",
            "message": "두 번째 데이터 목록 조회 성공",
            "data_list": secondary_list
        })

# ROUTE006 - 전체 두 번째 데이터 목록 조회 (시간 필터링 없음)
@IODataIntegration.route('/all_secondary_data_list')
class AllSecondaryDataList(Resource):
    def get(self):
        """
        시간 필터링 없이 모든 두 번째 데이터 목록 조회.
        
        Args:
            bucket (str): 기준 버킷 이름
            measurement (str): 기준 측정값 이름
            tag_key (str, optional): 태그 키
            tag_value (str, optional): 태그 값
            
        Returns:
            JSON: 시간 조건 없이 모든 유사 데이터 목록
            
        Process:
            1. 필수 매개변수 검증
            2. 기준 데이터 정보 구성
            3. 시간 제약 없이 모든 유사 데이터 조회
            4. 전체 유사 데이터 목록과 총 개수 반환
        """
        bucket = request.args.get('bucket')
        measurement = request.args.get('measurement')
        tag_key = request.args.get('tag_key')
        tag_value = request.args.get('tag_value')
        
        if not bucket:
            return jsonify({
                "status": "error",
                "message": "버킷 정보가 필요합니다",
                "data_list": []
            }), 400
            
        if not measurement:
            return jsonify({
                "status": "error",
                "message": "측정값 정보가 필요합니다",
                "data_list": []
            }), 400
        
        primary_data = {
            'bucket': bucket,
            'measurements': measurement
        }
        all_secondary_list = get_all_secondary_data_list(primary_data, tag_key, tag_value)
        
        return jsonify({
            "status": "success",
            "message": "모든 두 번째 데이터 목록 조회 성공 (시간 필터링 제외)",
            "data_list": all_secondary_list,
            "total_count": len(all_secondary_list)
        })

# ROUTE007 - 데이터 필드 목록 조회
@IODataIntegration.route('/field_list')
class FieldList(Resource):
    def post(self):
        """
        두 데이터셋의 필드(컬럼) 목록 조회.
        
        Args:
            Request Body JSON:
                data1 (dict): 첫 번째 데이터 정보 (bucket, measurement, tag_key, tag_value)
                data2 (dict): 두 번째 데이터 정보 (bucket, measurement, tag_key, tag_value)
                
        Returns:
            JSON: 각 데이터셋의 필드 목록
            
        Process:
            1. 요청 데이터 및 필수 필드 검증
            2. 각 데이터셋의 필드 목록 조회
            3. 두 데이터셋의 필드 정보를 결합하여 반환
        """
        req_data = request.get_json() if request.is_json else {}
        
        if not req_data or 'data1' not in req_data or 'data2' not in req_data:
            return jsonify({
                "status": "error",
                "message": "두 데이터 정보가 필요합니다",
                "fields": None
            }), 400
        
        data1 = req_data['data1']
        data2 = req_data['data2']
        
        required_fields = ['bucket', 'measurement']
        for field in required_fields:
            if field not in data1:
                return jsonify({
                    "status": "error",
                    "message": f"첫 번째 데이터의 {field} 정보가 필요합니다",
                    "fields": None
                }), 400
                
            if field not in data2:
                return jsonify({
                    "status": "error",
                    "message": f"두 번째 데이터의 {field} 정보가 필요합니다",
                    "fields": None
                }), 400
        
        tag_key1 = data1.get('tag_key')
        tag_value1 = data1.get('tag_value')
        tag_key2 = data2.get('tag_key')
        
        fields_data1 = get_fields(data1['bucket'], data1['measurement'], tag_key1, tag_value1)
        fields_data2 = get_fields(data2['bucket'], data2['measurement'], tag_key2)
        
        return jsonify({
            "status": "success",
            "message": "필드 목록 조회 성공",
            "fields": {
                "data1": fields_data1,
                "data2": fields_data2
            }
        })

# ROUTE008 - 도메인 목록 조회
@IODataIntegration.route('/domains')
class DomainList(Resource):
    def get(self):
        """
        시스템에서 사용 가능한 모든 도메인 목록 조회.
        
        Args:
            None
            
        Returns:
            JSON: 도메인 목록과 총 개수
            
        Process:
            1. 데이터베이스에서 도메인 목록 조회
            2. 도메인 목록과 개수 정보 반환
        """
        domain_list = get_domain_list()
        return jsonify({
            "status": "success",
            "domains": domain_list,
            "count": len(domain_list)
        })

# ROUTE009 - 태그 값 목록 조회
@IODataIntegration.route('/tag_values')
class TagValues(Resource):
    def get(self):
        """
        특정 버킷/측정값/태그키에 대한 태그 값 목록 조회.
        
        Args:
            bucket (str): 버킷 이름
            measurement (str): 측정값 이름
            tag_key (str): 태그 키
            
        Returns:
            JSON: 태그 값 목록 (ALL 옵션 포함)
            
        Process:
            1. 필수 매개변수 검증
            2. InfluxDB에서 태그 값 목록 조회
            3. ALL 옵션이 포함된 태그 값 목록 반환
        """
        bucket = request.args.get('bucket')
        measurement = request.args.get('measurement')
        tag_key = request.args.get('tag_key')
        
        if not bucket:
            return jsonify({
                "status": "error",
                "message": "버킷 정보가 필요합니다",
                "tag_values": []
            }), 400
            
        if not measurement:
            return jsonify({
                "status": "error",
                "message": "측정값 정보가 필요합니다",
                "tag_values": []
            }), 400
            
        if not tag_key:
            return jsonify({
                "status": "error",
                "message": "태그 키 정보가 필요합니다",
                "tag_values": []
            }), 400
        
        tag_values = get_tag_values(bucket, measurement, tag_key)
        
        return jsonify({
            "status": "success",
            "message": "태그 값 목록 조회 성공",
            "tag_values": tag_values,
            "count": len(tag_values)
        })

# FUNC001 - 통합 요청 데이터 검증
def validate_integration_request(request_data):
    """
    통합 요청 데이터의 유효성 검증.
    
    Args:
        request_data (dict): 검증할 요청 데이터
        
    Returns:
        dict: 검증 결과 {'is_valid': bool, 'message': str}
        
    Process:
        1. 필수 필드 존재 여부 확인
        2. 데이터 구조 및 형식 검증
        3. 시간 형식 및 범위 유효성 검증
        4. Period 값 유효성 검증
    """
    required_fields = ['base_data', 'selected_data', 'start_time', 'end_time', 'Period']
    
    for field in required_fields:
        if field not in request_data:
            return {
                'is_valid': False, 
                'message': f"필수 필드가 누락되었습니다: {field}"
            }
        
    base_data = request_data['base_data']
    selected_data = request_data['selected_data']
    
    if not isinstance(base_data, list) or len(base_data) < 2:
        return {
            'is_valid': False, 
            'message': "base_data 형식이 올바르지 않습니다. [bucket, measurement, tag_key, tag_value, features] 형식이어야 합니다."
        }
    
    if not isinstance(selected_data, list) or len(selected_data) < 2:
        return {
            'is_valid': False, 
            'message': "selected_data 형식이 올바르지 않습니다. [bucket, measurement, tag_key, tag_value, features] 형식이어야 합니다."
        }
    
    if not base_data[0] or not base_data[1]:
        return {
            'is_valid': False, 
            'message': "기준 데이터의 버킷과 측정값이 필요합니다."
        }
        
    if not selected_data[0] or not selected_data[1]:
        return {
            'is_valid': False, 
            'message': "선택 데이터의 버킷과 측정값이 필요합니다."
        }
        
    start_time = request_data['start_time']
    end_time = request_data['end_time']
    
    if not start_time or not end_time:
        return {
            'is_valid': False, 
            'message': "시작 시간과 종료 시간이 필요합니다."
        }
    
    try:
        start_date = start_time.split(' ')[0] if ' ' in start_time else start_time
        end_date = end_time.split(' ')[0] if ' ' in end_time else end_time
        
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        return {
            'is_valid': False, 
            'message': f"시간 형식이 올바르지 않습니다. YYYY-MM-DD 형식을 사용해주세요. 오류: {str(e)}"
        }
    
    period = request_data['Period']
    if period not in ['data1', 'data2']:
        return {
            'is_valid': False, 
            'message': "Period는 'data1' 또는 'data2' 값이어야 합니다."
        }
    
    return {'is_valid': True, 'message': "Valid"}

# FUNC002 - 통합 결과 요약 생성
def create_result_summary(result_data, request_data):
    """
    통합 결과에 대한 상세 요약 정보 생성.
    
    Args:
        result_data (dict): 통합 처리 결과 데이터
        request_data (dict): 원본 요청 데이터
        
    Returns:
        dict: 구조화된 요약 정보
        
    Process:
        1. 기준 데이터와 선택 데이터 정보 추출
        2. 통합 기간 및 필드 정보 정리
        3. 처리 통계 및 데이터셋 세부 정보 계산
        4. 전체 요약 정보 반환
    """
    base_data = request_data.get('base_data', [])
    selected_data = request_data.get('selected_data', [])
        
    summary = {
        'base_data_info': {
            'bucket': base_data[0] if len(base_data) > 0 else 'Unknown',
            'measurement': base_data[1] if len(base_data) > 1 else 'Unknown',
            'tag_filter': None
        },
        'selected_data_info': {
            'bucket': selected_data[0] if len(selected_data) > 0 else 'Unknown',
            'measurement': selected_data[1] if len(selected_data) > 1 else 'Unknown',
            'tag_filter': None
        },
        'integration_period': {
            'start_time': request_data.get('start_time'),
            'end_time': request_data.get('end_time'),
            'selected_period': request_data.get('Period', 'data1')
        },
        'field_info': {
            'selected_fields': [],
            'field_count': 0
        },
        'processing_stats': {
            'original_datasets': result_data.get('original_datasets', 0),
            'total_rows': result_data.get('total_original_rows', 0),
            'integration_frequency': result_data.get('integration_frequency', 'N/A'),
            'success': True
        }
    }
    
    if len(base_data) >= 4 and base_data[2] and base_data[3]:
        summary['base_data_info']['tag_filter'] = f"{base_data[2]}={base_data[3]}"
        
    if len(selected_data) >= 4 and selected_data[2] and selected_data[3]:
        summary['selected_data_info']['tag_filter'] = f"{selected_data[2]}={selected_data[3]}"
    
    if len(base_data) >= 5 and isinstance(base_data[4], list):
        summary['field_info']['selected_fields'] = base_data[4]
        summary['field_info']['field_count'] = len(base_data[4])
    
    if 'dataset_counts' in result_data:
        summary['dataset_details'] = []
        for dataset_name, count in result_data['dataset_counts'].items():
            integrated_count = result_data.get('integrated_stats', {}).get(dataset_name, count)
            reduction_rate = 0
            if count > 0:
                reduction_rate = max(0, (count - integrated_count) / count * 100)
            
            summary['dataset_details'].append({
                'name': dataset_name,
                'original_count': count,
                'integrated_count': integrated_count,
                'reduction_rate': round(reduction_rate, 2)
            })
    
    return summary
