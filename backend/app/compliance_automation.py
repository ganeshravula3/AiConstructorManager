"""
Compliance Automation Module

Automates regulatory compliance checks, document validation, and audit trail management
for construction projects.
"""
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import re

@dataclass
class ComplianceRule:
    rule_id: str
    rule_name: str
    regulation: str
    description: str
    severity: str  # 'warning', 'error', 'critical'
    check_function: str
    parameters: Dict[str, Any]
    active: bool

@dataclass
class ComplianceViolation:
    violation_id: str
    rule_id: str
    rule_name: str
    severity: str
    description: str
    detected_date: str
    resolved_date: Optional[str]
    status: str  # 'open', 'resolved', 'waived'
    context: Dict[str, Any]
    remediation_notes: str

@dataclass
class ComplianceCheck:
    check_id: str
    project_id: str
    check_date: str
    check_type: str
    status: str  # 'passed', 'failed', 'warning'
    violations_found: List[str]
    summary: str

class ComplianceAutomation:
    def __init__(self, storage_path: str = "backend/storage"):
        self.storage_path = Path(storage_path)
        self.rules_file = self.storage_path / "compliance_rules.json"
        self.violations_file = self.storage_path / "compliance_violations.json"
        self.checks_file = self.storage_path / "compliance_checks.json"
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize with default compliance rules
        self._initialize_default_rules()
    
    def _initialize_default_rules(self):
        """Initialize with standard construction compliance rules."""
        if not self.rules_file.exists():
            default_rules = self._create_default_rules()
            self._save_rules(default_rules)
    
    def _create_default_rules(self) -> List[ComplianceRule]:
        """Create default compliance rules for construction industry."""
        rules = [
            ComplianceRule(
                rule_id="gstin-validation",
                rule_name="GSTIN Format Validation",
                regulation="GST Act 2017",
                description="Validate GSTIN format and check digit",
                severity="error",
                check_function="validate_gstin_format",
                parameters={"require_active": True},
                active=True
            ),
            ComplianceRule(
                rule_id="invoice-amount-limit",
                rule_name="Invoice Amount Threshold Check",
                regulation="Income Tax Act",
                description="Check for invoices exceeding threshold limits",
                severity="warning",
                check_function="check_amount_threshold",
                parameters={"threshold": 200000, "currency": "INR"},
                active=True
            ),
            ComplianceRule(
                rule_id="tds-compliance",
                rule_name="TDS Deduction Compliance",
                regulation="Income Tax Act Section 194C",
                description="Ensure TDS is deducted for contractor payments above threshold",
                severity="error",
                check_function="check_tds_deduction",
                parameters={"threshold": 30000, "tds_rate": 0.01},
                active=True
            ),
            ComplianceRule(
                rule_id="documentation-completeness",
                rule_name="Required Documentation Check",
                regulation="Contract Law",
                description="Verify all required documents are present",
                severity="warning",
                check_function="check_documentation",
                parameters={"required_docs": ["invoice", "delivery_challan", "work_completion"]},
                active=True
            ),
            ComplianceRule(
                rule_id="payment-timeline",
                rule_name="Payment Timeline Compliance",
                regulation="MSME Act 2006",
                description="Ensure payments are made within regulatory timeframes",
                severity="error",
                check_function="check_payment_timeline",
                parameters={"max_days": 45},
                active=True
            ),
            ComplianceRule(
                rule_id="expense-categorization",
                rule_name="Proper Expense Categorization",
                regulation="Accounting Standards",
                description="Verify expenses are properly categorized",
                severity="warning",
                check_function="check_expense_category",
                parameters={"valid_categories": ["materials", "labor", "equipment", "overhead", "services"]},
                active=True
            ),
            ComplianceRule(
                rule_id="audit-trail",
                rule_name="Complete Audit Trail",
                regulation="Companies Act 2013",
                description="Ensure complete audit trail for all transactions",
                severity="critical",
                check_function="check_audit_trail",
                parameters={"required_fields": ["timestamp", "user", "action", "before", "after"]},
                active=True
            ),
            ComplianceRule(
                rule_id="duplicate-invoice",
                rule_name="Duplicate Invoice Detection",
                regulation="Internal Control",
                description="Detect potential duplicate invoice submissions",
                severity="error",
                check_function="check_duplicate_invoice",
                parameters={"check_fields": ["vendor", "invoice_number", "amount", "date"]},
                active=True
            )
        ]
        
        return rules
    
    def _save_rules(self, rules: List[ComplianceRule]):
        """Save compliance rules to storage."""
        data = []
        for rule in rules:
            data.append({
                'rule_id': rule.rule_id,
                'rule_name': rule.rule_name,
                'regulation': rule.regulation,
                'description': rule.description,
                'severity': rule.severity,
                'check_function': rule.check_function,
                'parameters': rule.parameters,
                'active': rule.active
            })
        
        with open(self.rules_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def load_rules(self) -> List[ComplianceRule]:
        """Load all compliance rules."""
        if not self.rules_file.exists():
            return []
        
        with open(self.rules_file, 'r') as f:
            data = json.load(f)
        
        rules = []
        for rule_data in data:
            rules.append(ComplianceRule(**rule_data))
        
        return rules
    
    def load_violations(self) -> List[ComplianceViolation]:
        """Load all compliance violations."""
        if not self.violations_file.exists():
            return []
        
        with open(self.violations_file, 'r') as f:
            data = json.load(f)
        
        violations = []
        for violation_data in data:
            violations.append(ComplianceViolation(**violation_data))
        
        return violations
    
    def save_violation(self, violation: ComplianceViolation):
        """Save a compliance violation."""
        violations = self.load_violations()
        violations.append(violation)
        
        data = []
        for v in violations:
            data.append({
                'violation_id': v.violation_id,
                'rule_id': v.rule_id,
                'rule_name': v.rule_name,
                'severity': v.severity,
                'description': v.description,
                'detected_date': v.detected_date,
                'resolved_date': v.resolved_date,
                'status': v.status,
                'context': v.context,
                'remediation_notes': v.remediation_notes
            })
        
        with open(self.violations_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def run_compliance_check(self, transaction_data: Dict[str, Any], 
                           project_id: str) -> Tuple[str, List[ComplianceViolation]]:
        """Run comprehensive compliance check on transaction data."""
        check_id = f"check-{uuid.uuid4().hex[:8]}"
        violations = []
        
        rules = [rule for rule in self.load_rules() if rule.active]
        
        for rule in rules:
            try:
                violation = self._execute_rule_check(rule, transaction_data, project_id)
                if violation:
                    violations.append(violation)
            except Exception as e:
                # Create a system error violation
                error_violation = ComplianceViolation(
                    violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                    rule_id=rule.rule_id,
                    rule_name=rule.rule_name,
                    severity="warning",
                    description=f"Rule execution failed: {str(e)}",
                    detected_date=datetime.now().isoformat(),
                    resolved_date=None,
                    status="open",
                    context={"error": str(e), "transaction": transaction_data},
                    remediation_notes="System administrator should review rule configuration"
                )
                violations.append(error_violation)
        
        # Save all violations
        for violation in violations:
            self.save_violation(violation)
        
        # Save compliance check record
        check_record = ComplianceCheck(
            check_id=check_id,
            project_id=project_id,
            check_date=datetime.now().isoformat(),
            check_type="transaction_validation",
            status="failed" if any(v.severity in ['error', 'critical'] for v in violations) else 
                   "warning" if violations else "passed",
            violations_found=[v.violation_id for v in violations],
            summary=f"Found {len(violations)} compliance issues"
        )
        
        self._save_check_record(check_record)
        
        return check_id, violations
    
    def _execute_rule_check(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                          project_id: str) -> Optional[ComplianceViolation]:
        """Execute a specific compliance rule check."""
        if rule.check_function == "validate_gstin_format":
            return self._check_gstin_format(rule, transaction_data, project_id)
        elif rule.check_function == "check_amount_threshold":
            return self._check_amount_threshold(rule, transaction_data, project_id)
        elif rule.check_function == "check_tds_deduction":
            return self._check_tds_deduction(rule, transaction_data, project_id)
        elif rule.check_function == "check_documentation":
            return self._check_documentation(rule, transaction_data, project_id)
        elif rule.check_function == "check_payment_timeline":
            return self._check_payment_timeline(rule, transaction_data, project_id)
        elif rule.check_function == "check_expense_category":
            return self._check_expense_category(rule, transaction_data, project_id)
        elif rule.check_function == "check_audit_trail":
            return self._check_audit_trail(rule, transaction_data, project_id)
        elif rule.check_function == "check_duplicate_invoice":
            return self._check_duplicate_invoice(rule, transaction_data, project_id)
        
        return None
    
    def _check_gstin_format(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                          project_id: str) -> Optional[ComplianceViolation]:
        """Check GSTIN format compliance."""
        gstin = transaction_data.get('vendor_gstin', '')
        
        if not gstin:
            return ComplianceViolation(
                violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                severity=rule.severity,
                description="GSTIN is missing from transaction",
                detected_date=datetime.now().isoformat(),
                resolved_date=None,
                status="open",
                context={"transaction_id": transaction_data.get('id', 'unknown')},
                remediation_notes="Obtain valid GSTIN from vendor"
            )
        
        # Basic GSTIN format validation (15 characters, alphanumeric)
        gstin_pattern = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$'
        
        if not re.match(gstin_pattern, gstin):
            return ComplianceViolation(
                violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                severity=rule.severity,
                description=f"Invalid GSTIN format: {gstin}",
                detected_date=datetime.now().isoformat(),
                resolved_date=None,
                status="open",
                context={"gstin": gstin, "transaction_id": transaction_data.get('id', 'unknown')},
                remediation_notes="Verify GSTIN format with vendor"
            )
        
        return None
    
    def _check_amount_threshold(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                              project_id: str) -> Optional[ComplianceViolation]:
        """Check if amount exceeds threshold."""
        amount = transaction_data.get('amount', 0)
        threshold = rule.parameters.get('threshold', 200000)
        
        if amount > threshold:
            return ComplianceViolation(
                violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                severity=rule.severity,
                description=f"Amount {amount} exceeds threshold of {threshold}",
                detected_date=datetime.now().isoformat(),
                resolved_date=None,
                status="open",
                context={"amount": amount, "threshold": threshold},
                remediation_notes="Additional documentation may be required for high-value transactions"
            )
        
        return None
    
    def _check_tds_deduction(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                           project_id: str) -> Optional[ComplianceViolation]:
        """Check TDS deduction compliance."""
        amount = transaction_data.get('amount', 0)
        threshold = rule.parameters.get('threshold', 30000)
        tds_amount = transaction_data.get('tds_deducted', 0)
        tds_rate = rule.parameters.get('tds_rate', 0.01)
        
        if amount > threshold:
            expected_tds = amount * tds_rate
            if abs(tds_amount - expected_tds) > (expected_tds * 0.1):  # 10% tolerance
                return ComplianceViolation(
                    violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                    rule_id=rule.rule_id,
                    rule_name=rule.rule_name,
                    severity=rule.severity,
                    description=f"TDS deduction mismatch: expected {expected_tds}, actual {tds_amount}",
                    detected_date=datetime.now().isoformat(),
                    resolved_date=None,
                    status="open",
                    context={"amount": amount, "expected_tds": expected_tds, "actual_tds": tds_amount},
                    remediation_notes="Verify TDS calculation and deduction"
                )
        
        return None
    
    def _check_documentation(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                           project_id: str) -> Optional[ComplianceViolation]:
        """Check required documentation."""
        required_docs = rule.parameters.get('required_docs', [])
        attached_docs = transaction_data.get('documents', [])
        
        missing_docs = []
        for required_doc in required_docs:
            if not any(doc.get('type', '').lower() == required_doc.lower() for doc in attached_docs):
                missing_docs.append(required_doc)
        
        if missing_docs:
            return ComplianceViolation(
                violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                severity=rule.severity,
                description=f"Missing required documents: {', '.join(missing_docs)}",
                detected_date=datetime.now().isoformat(),
                resolved_date=None,
                status="open",
                context={"missing_documents": missing_docs},
                remediation_notes="Obtain and attach missing documentation"
            )
        
        return None
    
    def _check_payment_timeline(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                              project_id: str) -> Optional[ComplianceViolation]:
        """Check payment timeline compliance."""
        transaction_date = transaction_data.get('date')
        payment_date = transaction_data.get('payment_date')
        max_days = rule.parameters.get('max_days', 45)
        
        if transaction_date and payment_date:
            trans_dt = datetime.fromisoformat(transaction_date)
            pay_dt = datetime.fromisoformat(payment_date)
            days_diff = (pay_dt - trans_dt).days
            
            if days_diff > max_days:
                return ComplianceViolation(
                    violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                    rule_id=rule.rule_id,
                    rule_name=rule.rule_name,
                    severity=rule.severity,
                    description=f"Payment delayed by {days_diff} days (limit: {max_days})",
                    detected_date=datetime.now().isoformat(),
                    resolved_date=None,
                    status="open",
                    context={"days_delayed": days_diff, "max_days": max_days},
                    remediation_notes="Improve payment processing to meet regulatory timelines"
                )
        
        return None
    
    def _check_expense_category(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                              project_id: str) -> Optional[ComplianceViolation]:
        """Check expense categorization."""
        category = transaction_data.get('category', '').lower()
        valid_categories = rule.parameters.get('valid_categories', [])
        
        if category not in valid_categories:
            return ComplianceViolation(
                violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                severity=rule.severity,
                description=f"Invalid expense category: {category}",
                detected_date=datetime.now().isoformat(),
                resolved_date=None,
                status="open",
                context={"category": category, "valid_categories": valid_categories},
                remediation_notes="Recategorize expense according to chart of accounts"
            )
        
        return None
    
    def _check_audit_trail(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                         project_id: str) -> Optional[ComplianceViolation]:
        """Check audit trail completeness."""
        audit_trail = transaction_data.get('audit_trail', [])
        required_fields = rule.parameters.get('required_fields', [])
        
        if not audit_trail:
            return ComplianceViolation(
                violation_id=f"viol-{uuid.uuid4().hex[:8]}",
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                severity=rule.severity,
                description="No audit trail found for transaction",
                detected_date=datetime.now().isoformat(),
                resolved_date=None,
                status="open",
                context={"transaction_id": transaction_data.get('id', 'unknown')},
                remediation_notes="Implement proper audit logging for all transactions"
            )
        
        return None
    
    def _check_duplicate_invoice(self, rule: ComplianceRule, transaction_data: Dict[str, Any], 
                               project_id: str) -> Optional[ComplianceViolation]:
        """Check for duplicate invoices."""
        # This would typically check against a database of existing transactions
        # For now, we'll return None (no duplicate found)
        # In a real implementation, you would check:
        # - Same vendor + invoice number
        # - Similar amounts and dates
        # - Exact matches across multiple fields
        
        return None
    
    def _save_check_record(self, check_record: ComplianceCheck):
        """Save compliance check record."""
        checks = []
        if self.checks_file.exists():
            with open(self.checks_file, 'r') as f:
                checks = json.load(f)
        
        checks.append({
            'check_id': check_record.check_id,
            'project_id': check_record.project_id,
            'check_date': check_record.check_date,
            'check_type': check_record.check_type,
            'status': check_record.status,
            'violations_found': check_record.violations_found,
            'summary': check_record.summary
        })
        
        with open(self.checks_file, 'w') as f:
            json.dump(checks, f, indent=2)
    
    def get_compliance_report(self, project_id: Optional[str] = None, 
                            days: int = 30) -> Dict[str, Any]:
        """Generate compliance report."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        violations = self.load_violations()
        
        # Filter by project and date range
        filtered_violations = []
        for v in violations:
            v_date = datetime.fromisoformat(v.detected_date)
            if start_date <= v_date <= end_date:
                if not project_id or v.context.get('project_id') == project_id:
                    filtered_violations.append(v)
        
        # Calculate statistics
        total_violations = len(filtered_violations)
        open_violations = len([v for v in filtered_violations if v.status == 'open'])
        critical_violations = len([v for v in filtered_violations if v.severity == 'critical'])
        error_violations = len([v for v in filtered_violations if v.severity == 'error'])
        warning_violations = len([v for v in filtered_violations if v.severity == 'warning'])
        
        # Violations by rule
        rule_breakdown = {}
        for v in filtered_violations:
            rule_breakdown[v.rule_name] = rule_breakdown.get(v.rule_name, 0) + 1
        
        return {
            'report_period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': days
            },
            'project_id': project_id,
            'summary': {
                'total_violations': total_violations,
                'open_violations': open_violations,
                'resolved_violations': total_violations - open_violations,
                'critical_violations': critical_violations,
                'error_violations': error_violations,
                'warning_violations': warning_violations
            },
            'violations_by_rule': rule_breakdown,
            'violations': [
                {
                    'violation_id': v.violation_id,
                    'rule_name': v.rule_name,
                    'severity': v.severity,
                    'description': v.description,
                    'detected_date': v.detected_date,
                    'status': v.status
                }
                for v in filtered_violations
            ],
            'generated_at': datetime.now().isoformat()
        }
    
    def resolve_violation(self, violation_id: str, remediation_notes: str = "") -> bool:
        """Mark a violation as resolved."""
        violations = self.load_violations()
        
        for violation in violations:
            if violation.violation_id == violation_id:
                violation.status = 'resolved'
                violation.resolved_date = datetime.now().isoformat()
                violation.remediation_notes = remediation_notes
                
                # Save updated violations
                data = []
                for v in violations:
                    data.append({
                        'violation_id': v.violation_id,
                        'rule_id': v.rule_id,
                        'rule_name': v.rule_name,
                        'severity': v.severity,
                        'description': v.description,
                        'detected_date': v.detected_date,
                        'resolved_date': v.resolved_date,
                        'status': v.status,
                        'context': v.context,
                        'remediation_notes': v.remediation_notes
                    })
                
                with open(self.violations_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                return True
        
        return False