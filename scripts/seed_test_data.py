"""
Seed Test Student Data into PostgreSQL.

Inserts realistic academic data for ~20 students so the AI chat assistant
can demonstrate answering student-related questions.

Run: python scripts/seed_test_data.py
"""

import sys, os, random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common import database as db

# ── Test students from MSRIT CS dept (2021 batch) ────────────────────────────
STUDENTS = [
    ("1MS21CS001", "Aarav Sharma",    "aarav.sharma@msrit.edu",   [8.2, 8.5, 7.9, 8.8, 9.1, 8.7]),
    ("1MS21CS002", "Bhavya Reddy",    "bhavya.r@msrit.edu",       [7.5, 6.8, 7.2, 7.0, 6.5, 7.1]),
    ("1MS21CS003", "Chetan Kumar",    "chetan.k@msrit.edu",       [6.0, 5.5, 4.8, 5.2, 4.5, 0.0]),  # backlog
    ("1MS21CS004", "Divya Nair",      "divya.n@msrit.edu",        [9.2, 9.5, 9.3, 9.7, 9.8, 9.6]),  # topper
    ("1MS21CS005", "Eshan Patel",     "eshan.p@msrit.edu",        [8.0, 8.3, 7.8, 8.6, 8.1, 8.4]),
    ("1MS21CS006", "Fathima Hassan",  "fathima.h@msrit.edu",      [7.2, 7.5, 7.0, 7.8, 7.3, 0.0]),
    ("1MS21CS007", "Gaurav Singh",    "gaurav.s@msrit.edu",       [5.5, 4.8, 4.2, 4.0, 3.8, 0.0]),  # backlog in 4,5
    ("1MS21CS008", "Hema Lakshmi",    "hema.l@msrit.edu",         [8.8, 8.6, 9.0, 8.9, 8.7, 9.2]),
    ("1MS21CS009", "Ishan Mehta",     "ishan.m@msrit.edu",        [6.5, 7.0, 6.8, 7.2, 7.5, 7.8]),
    ("1MS21CS010", "Jasmine Kaur",    "jasmine.k@msrit.edu",      [7.8, 8.0, 8.2, 7.9, 8.5, 8.3]),
    ("1MS21CS011", "Kiran Rao",       "kiran.r@msrit.edu",        [4.5, 4.0, 3.5, 3.8, 0.0, 0.0]),  # multiple backlogs
    ("1MS21CS012", "Lakshmi Devi",    "lakshmi.d@msrit.edu",      [8.1, 7.9, 8.3, 8.0, 8.4, 8.6]),
    ("1MS21CS013", "Mohit Joshi",     "mohit.j@msrit.edu",        [9.0, 9.3, 9.1, 9.5, 9.4, 9.6]),
    ("1MS21CS014", "Neha Gupta",      "neha.g@msrit.edu",         [7.0, 7.3, 7.1, 7.5, 7.2, 7.4]),
    ("1MS21CS015", "Om Prakash",      "om.p@msrit.edu",           [5.0, 4.5, 4.8, 5.2, 4.0, 0.0]),  # backlog in sem 5
    ("1MS21CS016", "Priya Venkat",    "priya.v@msrit.edu",        [8.5, 8.7, 8.9, 8.6, 9.0, 8.8]),
    ("1MS21CS017", "Rahul Das",       "rahul.d@msrit.edu",        [6.8, 7.1, 7.3, 6.9, 7.4, 7.6]),
    ("1MS21CS018", "Sneha Jain",      "sneha.j@msrit.edu",        [9.1, 9.4, 9.2, 9.6, 9.5, 9.7]),  # 2nd topper
    ("1MS21CS019", "Tarun Bhat",      "tarun.b@msrit.edu",        [3.8, 4.2, 3.5, 4.0, 0.0, 0.0]),  # many backlogs
    ("1MS21CS020", "Uma Shankar",     "uma.s@msrit.edu",          [7.6, 7.9, 7.7, 8.1, 7.8, 8.0]),
]

SUBJECTS_BY_SEM = {
    1: [("CS01", "Engineering Mathematics-I"),     ("CS02", "Programming in C"),
        ("CS03", "Basic Electronics"),              ("CS04", "Engineering Physics")],
    2: [("CS05", "Engineering Mathematics-II"),    ("CS06", "Data Structures"),
        ("CS07", "Digital Systems"),               ("CS08", "Object Oriented Programming")],
    3: [("CS09", "Discrete Mathematics"),          ("CS10", "Design & Analysis of Algorithms"),
        ("CS11", "Database Management Systems"),   ("CS12", "Computer Organization")],
    4: [("CS13", "Operating Systems"),             ("CS14", "Computer Networks"),
        ("CS15", "Theory of Computation"),         ("CS16", "Software Engineering")],
    5: [("CS17", "Compiler Design"),               ("CS18", "Artificial Intelligence"),
        ("CS19", "Machine Learning"),              ("CS20", "Web Technologies")],
    6: [("CS21", "Cloud Computing"),               ("CS22", "Cryptography & Security"),
        ("CS23", "Mobile Application Development"),("CS24", "Big Data Analytics")],
}


def gp_to_marks(gp: float) -> tuple[int, str, str]:
    """Convert grade points to marks, letter grade, and PASS/FAIL."""
    if gp <= 0:
        return 0, "F", "FAIL"
    elif gp >= 9.5:
        return 95, "O",  "PASS"
    elif gp >= 9.0:
        return 90, "A+", "PASS"
    elif gp >= 8.5:
        return 85, "A",  "PASS"
    elif gp >= 8.0:
        return 80, "B+", "PASS"
    elif gp >= 7.5:
        return 75, "B",  "PASS"
    elif gp >= 7.0:
        return 70, "C+", "PASS"
    elif gp >= 6.5:
        return 65, "C",  "PASS"
    elif gp >= 6.0:
        return 60, "D",  "PASS"
    elif gp >= 5.0:
        return 50, "E",  "PASS"
    elif gp >= 4.0:
        return 40, "E",  "PASS"
    else:
        return 35, "F",  "FAIL"


def main():
    print("Initialising database connection …")
    db.init_db()
    institution_id = db.get_default_institution_id()
    print(f"Institution ID: {institution_id}")

    total_students = 0
    total_results  = 0

    for usn, name, email, sgpa_list in STUDENTS:
        student_id = db.upsert_student(usn, name, email=email, institution_id=institution_id, source="seed")
        total_students += 1
        print(f"  Student: {usn} – {name}")

        for sem_idx, sgpa in enumerate(sgpa_list, start=1):
            subjects = SUBJECTS_BY_SEM.get(sem_idx, [])
            backlogs_in_sem = 0

            for subj_code, subj_name in subjects:
                # Vary marks slightly per subject around the semester SGPA
                random.seed(hash(usn + subj_code))
                variation = random.uniform(-0.5, 0.5)
                subj_gp = max(0.0, min(10.0, sgpa + variation)) if sgpa > 0 else 0.0

                marks, grade, status = gp_to_marks(subj_gp)
                if status == "FAIL":
                    backlogs_in_sem += 1

                subject_id = db.get_or_create_subject(institution_id, subj_code, subj_name, sem_idx)
                db.upsert_result(
                    student_id=student_id,
                    subject_id=subject_id,
                    semester=sem_idx,
                    marks_obtained=float(marks),
                    max_marks=100.0,
                    grade=grade,
                    grade_points=subj_gp if sgpa > 0 else 0.0,
                    status=status,
                    exam_type="regular",
                )
                total_results += 1

            if sgpa > 0:
                db.store_semester_aggregate(
                    student_id=student_id,
                    semester=sem_idx,
                    sgpa=sgpa,
                    credits_earned=len(subjects) * 3,
                    backlogs=backlogs_in_sem,
                )

        db.compute_and_store_cgpa(student_id)

    print(f"\nSEEDED {total_students} students, {total_results} result records into PostgreSQL.")
    print("\nQuick stats:")

    # Show what was inserted
    students_db = db.get_all_students(limit=100)
    for s in students_db:
        cgpa = s.get("cgpa") or 0
        display_name = s.get("full_name") or s.get("name") or s.get("usn") or ""
        results = db.get_student_results(s["usn"])
        backlogs = sum(1 for r in results if r.get("status","").upper() == "FAIL")
        print(f"  {s['usn']:15s}  {display_name:20s}  CGPA={cgpa:.2f}  backlogs={backlogs}")


if __name__ == "__main__":
    main()
