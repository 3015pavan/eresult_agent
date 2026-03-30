"""
Universal Document Extractor - Handles Any Institution & Document Type

Advanced extraction system that can process:
- Structured/unstructured PDFs
- Different institution formats
- Various document types (Excel, PDF, Word, HTML, Images)
- Multiple grading systems and USN formats
"""

import json
import logging
import os
import re
from typing import Any, Optional, List, Dict, Tuple
import httpx

logger = logging.getLogger(__name__)

# Institution patterns and formats
_INSTITUTION_PATTERNS = {
    # VTU/Visvesvaraya
    'vtu': {
        'usn_pattern': re.compile(r'\b([1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3})\b', re.IGNORECASE),
        'name': 'VTU',
        'grades': ['O', 'A+', 'A', 'B+', 'B', 'C', 'P', 'F'],
        'grade_points': {'O': 10, 'A+': 9, 'A': 8, 'B+': 7, 'B': 6, 'C': 5, 'P': 4, 'F': 0}
    },
    
    # MSRIT (already optimized)
    'msrit': {
        'usn_pattern': re.compile(r'\b(1MS\d{2}CS\d{3})\b', re.IGNORECASE),
        'name': 'MSRIT',
        'grades': ['O', 'A+', 'A', 'B+', 'B', 'C', 'P', 'F', 'NE', 'W'],
        'grade_points': {'O': 10, 'A+': 9, 'A': 8, 'B+': 7, 'B': 6, 'C': 5, 'P': 4, 'F': 0, 'NE': 0, 'W': 0}
    },
    
    # Autonomous Colleges (general)
    'autonomous': {
        'usn_pattern': re.compile(r'\b([A-Z]{2,4}\d{2,4}[A-Z]{2,4}\d{3,4})\b', re.IGNORECASE),
        'name': 'Autonomous',
        'grades': ['O', 'A+', 'A', 'B+', 'B', 'C', 'P', 'F'],
        'grade_points': {'O': 10, 'A+': 9, 'A': 8, 'B+': 7, 'B': 6, 'C': 5, 'P': 4, 'F': 0}
    },
    
    # University systems (general)
    'university': {
        'usn_pattern': re.compile(r'\b([A-Z0-9]{6,12})\b', re.IGNORECASE),
        'name': 'University',
        'grades': ['A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D', 'F'],
        'grade_points': {'A+': 10, 'A': 9, 'A-': 8, 'B+': 7, 'B': 6, 'B-': 5, 'C+': 4, 'C': 3, 'C-': 2, 'D': 1, 'F': 0}
    }
}

# Document structure patterns
_STRUCTURE_INDICATORS = [
    'result', 'grade', 'mark', 'score', 'semester', 'term', 'exam',
    'student', 'usn', 'register', 'roll', 'name', 'subject',
    'sgpa', 'cgpa', 'gpa', 'percentage', '%'
]

# Universal system prompt
_UNIVERSAL_PROMPT = """You are a universal academic result extraction engine.

CAPABILITIES:
- Extract from ANY institution format (VTU, MSRIT, Autonomous, University, etc.)
- Handle structured/unstructured PDFs, Excel, Word, HTML documents
- Support multiple grading systems (letter grades, points, percentages)
- Process various USN/register number formats

EXTRACTION RULES:
1. Return JSON array of student result objects
2. Each object must have:
{
  "usn": "string (student identifier/register number)",
  "name": "string (student name)",
  "institution": "string (institution name if identifiable)",
  "semester": integer (1-8 or term identifier),
  "sgpa": float or null (0.0-10.0 scale if applicable),
  "cgpa": float or null (cumulative GPA if applicable),
  "academic_year": "string (e.g. 2023-24)",
  "exam_type": "regular|supplementary|improvement|backlog",
  "subjects": [{
    "subject_code": "string (course code)",
    "subject_name": "string (course title)",
    "internal_marks": integer or null,
    "external_marks": integer or null,
    "total_marks": integer or null,
    "max_marks": integer (default 100),
    "grade": "string (letter grade or points)",
    "grade_points": float (0.0-10.0 scale),
    "credits": integer (default 3),
    "status": "PASS|FAIL|ABSENT|WITHHELD"
  }]
}

3. ADAPTIVE PROCESSING:
   - Detect institution type from document patterns
   - Handle different USN formats automatically
   - Convert various grade systems to 10-point scale
   - Process both structured tables and unstructured text
   - Extract from PDFs with/without text layers

4. FLEXIBLE HANDLING:
   - If document is unstructured: extract any student data visible
   - If grades are percentages: convert to letter grades
   - If multiple grading systems: normalize to 10-point scale
   - If institution unknown: use "Unknown" and extract generically

5. RETURN [] only if no academic/student data is present

Document text:
{text[:4000]}"""

class UniversalExtractor:
    """Universal extractor for any institution and document type"""
    
    def __init__(self):
        # Load API configuration
        try:
            from src.common.config import get_settings
            settings = get_settings()
            self.api_key = settings.llm.groq_api_key
            self.model = settings.llm.groq_model
        except (ImportError, AttributeError):
            self.api_key = os.getenv("GROQ_API_KEY", "")
            self.model = "llama-3.3-70b-versatile"
        
        self.client = httpx.Client(timeout=30.0)
    
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    def _detect_institution(self, text: str) -> str:
        """Detect institution type from document patterns"""
        text_lower = text.lower()
        
        # Check for specific institution markers
        if 'msrit' in text_lower or 'ramaiah' in text_lower:
            return 'msrit'
        elif 'vtu' in text_lower or 'visvesvaraya' in text_lower:
            return 'vtu'
        elif any(keyword in text_lower for keyword in ['autonomous', 'autonomous institute']):
            return 'autonomous'
        else:
            return 'university'
    
    def _extract_usns(self, text: str, institution: str) -> List[str]:
        """Extract USNs based on institution pattern"""
        pattern = _INSTITUTION_PATTERNS[institution]['usn_pattern']
        return list(set(pattern.findall(text)))
    
    def _normalize_grade(self, grade: str, institution: str) -> Tuple[str, float]:
        """Normalize grade to letter and points based on institution"""
        if not grade:
            return "", 0.0
        
        grade = str(grade).strip().upper()
        inst_grades = _INSTITUTION_PATTERNS[institution]['grades']
        inst_points = _INSTITUTION_PATTERNS[institution]['grade_points']
        
        # Direct match
        if grade in inst_grades:
            return grade, inst_points[grade]
        
        # Handle percentage grades
        if grade.endswith('%') or grade.isdigit():
            try:
                pct = float(grade.rstrip('%'))
                if pct >= 90: return 'O', 10.0
                elif pct >= 80: return 'A+', 9.0
                elif pct >= 70: return 'A', 8.0
                elif pct >= 60: return 'B+', 7.0
                elif pct >= 55: return 'B', 6.0
                elif pct >= 50: return 'C', 5.0
                elif pct >= 40: return 'P', 4.0
                else: return 'F', 0.0
            except ValueError:
                pass
        
        # Handle numeric grade points
        try:
            points = float(grade)
            if 0 <= points <= 10:
                # Convert points back to letter grade
                for letter, pt in inst_points.items():
                    if abs(points - pt) < 0.5:
                        return letter, pt
        except ValueError:
            pass
        
        return grade, 0.0
    
    def _call_api(self, prompt: str) -> Optional[str]:
        """Call LLM API with universal prompt"""
        if not self.api_key:
            logger.warning("No API key configured")
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
                        {"role": "system", "content": _UNIVERSAL_PROMPT},
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
            logger.warning(f"API error: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"API call failed: {e}")
            return None
    
    def _parse_response(self, raw: str) -> List[Dict]:
        """Parse and validate universal response"""
        if not raw:
            return []
        
        try:
            cleaned = re.sub(r'```(?:json)?\s*', '', raw).strip().rstrip('`').strip()
            parsed = json.loads(cleaned)
            
            if isinstance(parsed, list):
                return parsed
            elif isinstance(parsed, dict):
                for key in ('results', 'data', 'students', 'records'):
                    if isinstance(parsed.get(key), list):
                        return parsed[key]
                return [parsed]
            return []
        
        except json.JSONDecodeError:
            match = re.search(r'\[.*\]', raw, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    pass
            return []
    
    def _enhance_with_patterns(self, records: List[Dict], text: str, institution: str) -> List[Dict]:
        """Enhance extracted records with institution-specific patterns"""
        enhanced_records = []
        
        for record in records:
            # Add institution info
            record['institution'] = _INSTITUTION_PATTERNS[institution]['name']
            
            # Normalize grades and grade points
            for subject in record.get('subjects', []):
                grade = subject.get('grade', '')
                if grade:
                    norm_grade, norm_points = self._normalize_grade(grade, institution)
                    subject['original_grade'] = grade
                    subject['grade'] = norm_grade
                    subject['grade_points'] = norm_points
            
            # Validate USN format
            usn = record.get('usn', '')
            if usn:
                pattern = _INSTITUTION_PATTERNS[institution]['usn_pattern']
                if pattern.fullmatch(usn):
                    record['usn_valid'] = True
                else:
                    record['usn_valid'] = False
            
            record['extraction_strategy'] = 'universal_llm'
            record['overall_confidence'] = 0.85
            
            enhanced_records.append(record)
        
        return enhanced_records
    
    def extract(self, text: str) -> List[Dict]:
        """Universal extraction method"""
        # Detect institution
        institution = self._detect_institution(text)
        logger.info(f"Detected institution: {institution}")
        
        # Extract USNs for validation
        usns = self._extract_usns(text, institution)
        logger.info(f"Found USNs: {len(usns)}")
        
        # Create enhanced prompt
        prompt = f"""Extract academic result data from this document.

Institution Type: {institution}
USNs Found: {', '.join(usns[:5])}
Document Type: {'Structured' if any(ind in text.lower() for ind in _STRUCTURE_INDICATORS) else 'Unstructured'}

{text[:4000]}"""
        
        # Call API
        raw_response = self._call_api(prompt)
        if not raw_response:
            return []
        
        # Parse response
        records = self._parse_response(raw_response)
        
        # Enhance with institution patterns
        enhanced_records = self._enhance_with_patterns(records, text, institution)
        
        logger.info(f"Universal extraction: {len(enhanced_records)} records")
        return enhanced_records
    
    def extract_with_fallback(self, text: str) -> List[Dict]:
        """Extract with multiple fallback strategies"""
        try:
            # Try universal extraction first
            results = self.extract(text)
            if results:
                return results
        except Exception as e:
            logger.warning(f"Universal extraction failed: {e}")
        
        # Fallback to enhanced MSRIT extractor
        try:
            from .enhanced_llm_extractor import create_enhanced_extractor
            enhanced_extractor = create_enhanced_extractor()
            logger.info("Falling back to enhanced MSRIT extractor")
            return enhanced_extractor.extract_with_fallback(text)
        except Exception as e:
            logger.warning(f"Enhanced MSRIT fallback failed: {e}")
        
        # Final fallback to original LLM
        try:
            from .llm_extractor import llm_extract
            logger.info("Falling back to original LLM extractor")
            return llm_extract(text)
        except Exception as e:
            logger.error(f"All extraction strategies failed: {e}")
            return []

# Factory function
def create_universal_extractor() -> UniversalExtractor:
    """Create universal extractor instance"""
    return UniversalExtractor()
