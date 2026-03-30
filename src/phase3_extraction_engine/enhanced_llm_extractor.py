"""
Enhanced LLM Extractor - Optimized for MSRIT CS UG Result Sheets

Production-ready extraction system with MSRIT-specific patterns and optimized performance.
"""

import json
import logging
import os
import re
from typing import Any, Optional, List, Dict
import httpx

logger = logging.getLogger(__name__)

# Optimized MSRIT subject patterns (consolidated and cleaned)
_MSrit_SUBJECT_CODES = {
    # Core CS subjects
    'CS24', 'CS31', 'CS32', 'CS33', 'CS34', 'CS35',
    'CS51', 'CS52', 'CS53', 'CS54', 'CS55', 'CS61', 'CS62', 'CS63',
    'CS71', 'CS72', 'CS73', 'CSL18', 'CSL28',
    # Engineering subjects
    'CV101', 'CV14', 'EC101', 'EC23', 'EC25(O)', 'EE101', 'EE13', 'EE201',
    'CYL17', 'CY25', 'CYL27', 'MA21', 'MA21(O)',
    # Humanities and electives
    'HS12', 'HS391K', 'HS391M', 'HS59', 'AEC16', 'AEC26',
    'AL58', 'AL61', 'UHV38',
    # Open electives (simplified)
    'BTOE01', 'CHOE01', 'IMOE01', 'CSOE02', 'CVOE01', 'MEOE08', 'ECOE03',
    # Project/internship codes
    '19CSIN', '19CSP', '21YO83', '21PE83', '21NS83', '21INT82', '21CSP81'
}

# MSRIT USN pattern
_MSrit_USN_PATTERN = re.compile(r'\b(1MS\d{2}CS\d{3})\b', re.IGNORECASE)

# Optimized system prompt
_ENHANCED_PROMPT = """You are an academic result extraction engine specialized in MSRIT VTU grade reports.

EXTRACT ONLY VALID MSRIT DATA:
- USN format: 1MS[YY]CS[###] (e.g., 1MS21CS001)
- Valid subjects: CS24, CS31, CS32, CS33, CS34, CS35, CS51, CS52, CS53, CS54, CS55, CS61, CS62, CS63, CS71, CS72, CS73, CSL18, CSL28, CV101, CV14, EC101, EC23, EE101, EE201, CYL17, CY25, MA21, HS12, AEC16, etc.
- Grades: O, A+, A, B+, B, C, P, F, NE, W

RETURN JSON ARRAY:
[{
  "usn": "string",
  "name": "string", 
  "semester": integer,
  "sgpa": float or null,
  "cgpa": float or null,
  "academic_year": "string",
  "exam_type": "regular|supplementary|improvement",
  "subjects": [{
    "subject_code": "string",
    "subject_name": "string",
    "internal_marks": integer or null,
    "external_marks": integer or null,
    "total_marks": integer or null,
    "max_marks": integer,
    "grade": "O|A+|A|B+|B|C|P|F|NE|W",
    "grade_points": float,
    "credits": integer,
    "status": "PASS|FAIL|ABSENT|WITHHELD"
  }]
}]

CRITICAL RULES:
1. Return ONLY JSON array, no explanation
2. Use exact MSRIT subject codes from above list
3. Validate USN format: 1MS[YY]CS[###]
4. Grade points: O=10, A+=9, A=8, B+=7, B=6, C=5, P=4, F=0, NE=0, W=0
5. Status: PASS if grade in O,A+,A,B+,B,C,P else FAIL
6. SGPA: compute from grade_points × credits / total credits
7. Return [] if no valid MSRIT data found

{text[:4000]}"""

class EnhancedLLMExtractor:
    """Production-ready enhanced LLM extractor for MSRIT results"""
    
    def __init__(self):
        # Load API key from system config
        try:
            from src.common.config import get_settings
            settings = get_settings()
            self.api_key = settings.llm.groq_api_key
            self.model = settings.llm.groq_model
        except (ImportError, AttributeError):
            # Fallback
            self.api_key = os.getenv("GROQ_API_KEY", "")
            self.model = "llama-3.3-70b-versatile"
        
        self.client = httpx.Client(timeout=30.0)
    
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    def _preprocess_text(self, text: str) -> str:
        """Optimized text preprocessing for MSRIT patterns"""
        # Highlight USNs
        text = _MSrit_USN_PATTERN.sub(lambda m: f"USN: {m.group().upper()}", text)
        
        # Highlight subject codes
        for code in _MSrit_SUBJECT_CODES:
            if code in text:
                text = text.replace(code, f"SUBJECT_{code}")
        
        # Normalize semester numbers
        text = re.sub(r'Semester\s*[:\-]?\s*[IVX]+', 
                     lambda m: f"Semester: {self._roman_to_int(m.group().split(':')[-1].strip())}", 
                     text, flags=re.IGNORECASE)
        
        return text
    
    def _roman_to_int(self, roman: str) -> int:
        """Convert Roman numeral to integer"""
        roman_map = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6, 'VII': 7, 'VIII': 8}
        return roman_map.get(roman.upper().strip(), 1)
    
    def _call_api(self, prompt: str) -> Optional[str]:
        """Optimized API call with error handling"""
        if not self.api_key:
            logger.warning("No GROQ API key configured")
            return None
        
        try:
            response = self.client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": _ENHANCED_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 4096,
                    "response_format": {"type": "json_object"},
                },
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        
        except httpx.HTTPStatusError as e:
            logger.warning(f"GROQ API error: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"API call failed: {e}")
            return None
    
    def _parse_response(self, raw: str) -> List[Dict]:
        """Parse and validate LLM response"""
        if not raw:
            return []
        
        try:
            # Clean response
            cleaned = re.sub(r'```(?:json)?\s*', '', raw).strip().rstrip('`').strip()
            parsed = json.loads(cleaned)
            
            if isinstance(parsed, list):
                return parsed
            elif isinstance(parsed, dict) and 'results' in parsed:
                return parsed['results'] if isinstance(parsed['results'], list) else [parsed['results']]
            else:
                return [parsed]
        
        except json.JSONDecodeError:
            # Try to extract JSON array
            match = re.search(r'\[.*\]', raw, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    pass
            return []
    
    def _validate_record(self, record: Dict) -> bool:
        """Validate extracted record against MSRIT patterns"""
        # Check USN format
        usn = record.get('usn', '')
        if not _MSrit_USN_PATTERN.fullmatch(usn):
            return False
        
        # Check semester
        semester = record.get('semester')
        if not isinstance(semester, int) or not (1 <= semester <= 8):
            return False
        
        # Check subjects
        subjects = record.get('subjects', [])
        if not subjects:
            return False
        
        # Validate at least one subject code
        valid_subjects = False
        for subj in subjects:
            code = subj.get('subject_code', '')
            if code in _MSrit_SUBJECT_CODES:
                valid_subjects = True
                break
        
        return valid_subjects
    
    def extract(self, text: str) -> List[Dict]:
        """Main extraction method"""
        # Preprocess text
        enhanced_text = self._preprocess_text(text)
        
        # Create prompt
        prompt = f"Extract MSRIT student result data from the following text:\n\n{enhanced_text}"
        
        # Call API
        raw_response = self._call_api(prompt)
        if not raw_response:
            return []
        
        # Parse response
        records = self._parse_response(raw_response)
        
        # Validate and filter records
        valid_records = []
        for record in records:
            if self._validate_record(record):
                # Add extraction metadata
                record['extraction_strategy'] = 'enhanced_llm'
                record['overall_confidence'] = 0.90
                valid_records.append(record)
        
        logger.info(f"Enhanced LLM extracted {len(valid_records)} valid records")
        return valid_records
    
    def extract_with_fallback(self, text: str) -> List[Dict]:
        """Extract with fallback to original LLM"""
        try:
            # Try enhanced extraction first
            results = self.extract(text)
            if results:
                return results
        except Exception as e:
            logger.warning(f"Enhanced extraction failed: {e}")
        
        # Fallback to original LLM
        try:
            from .llm_extractor import llm_extract
            logger.info("Falling back to original LLM extractor")
            return llm_extract(text)
        except Exception as e:
            logger.error(f"Fallback extraction failed: {e}")
            return []

# Factory function
def create_enhanced_extractor() -> EnhancedLLMExtractor:
    """Create enhanced extractor instance"""
    return EnhancedLLMExtractor()
