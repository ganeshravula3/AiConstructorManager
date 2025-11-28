"""
Budget Tracking and Analytics Module

Provides project budget monitoring, spend analysis, and cost overrun alerts.
"""
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Budget:
    project_id: str
    total_budget: float
    allocated_amounts: Dict[str, float]  # category -> amount
    spent_amounts: Dict[str, float]     # category -> spent
    created_date: str
    last_updated: str

@dataclass
class BudgetAlert:
    alert_id: str
    project_id: str
    category: str
    alert_type: str  # 'warning', 'critical', 'overrun'
    message: str
    percentage_used: float
    created_date: str

class BudgetTracker:
    def __init__(self, storage_path: str = "backend/storage"):
        self.storage_path = Path(storage_path)
        self.budgets_file = self.storage_path / "budgets.json"
        self.alerts_file = self.storage_path / "budget_alerts.json"
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
    def load_budgets(self) -> Dict[str, Budget]:
        """Load all project budgets from storage."""
        if not self.budgets_file.exists():
            return {}
        
        with open(self.budgets_file, 'r') as f:
            data = json.load(f)
            
        budgets = {}
        for proj_id, budget_data in data.items():
            budgets[proj_id] = Budget(**budget_data)
        return budgets
    
    def save_budgets(self, budgets: Dict[str, Budget]):
        """Save budgets to storage."""
        data = {}
        for proj_id, budget in budgets.items():
            data[proj_id] = {
                'project_id': budget.project_id,
                'total_budget': budget.total_budget,
                'allocated_amounts': budget.allocated_amounts,
                'spent_amounts': budget.spent_amounts,
                'created_date': budget.created_date,
                'last_updated': budget.last_updated
            }
        
        with open(self.budgets_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def create_budget(self, project_id: str, total_budget: float, 
                     category_allocations: Dict[str, float]) -> Budget:
        """Create a new project budget."""
        now = datetime.now().isoformat()
        
        # Validate allocations don't exceed total
        total_allocated = sum(category_allocations.values())
        if total_allocated > total_budget:
            raise ValueError(f"Total allocations ({total_allocated}) exceed budget ({total_budget})")
        
        budget = Budget(
            project_id=project_id,
            total_budget=total_budget,
            allocated_amounts=category_allocations,
            spent_amounts={cat: 0.0 for cat in category_allocations.keys()},
            created_date=now,
            last_updated=now
        )
        
        budgets = self.load_budgets()
        budgets[project_id] = budget
        self.save_budgets(budgets)
        
        return budget
    
    def add_expense(self, project_id: str, category: str, amount: float, 
                   description: str = "") -> Dict[str, Any]:
        """Add an expense to a project budget category."""
        budgets = self.load_budgets()
        
        if project_id not in budgets:
            raise ValueError(f"Project {project_id} not found")
        
        budget = budgets[project_id]
        
        if category not in budget.allocated_amounts:
            raise ValueError(f"Category {category} not found in budget")
        
        # Update spent amount
        budget.spent_amounts[category] += amount
        budget.last_updated = datetime.now().isoformat()
        
        budgets[project_id] = budget
        self.save_budgets(budgets)
        
        # Check for budget alerts
        alerts = self._check_budget_alerts(budget, category)
        
        return {
            'project_id': project_id,
            'category': category,
            'amount_added': amount,
            'new_spent_total': budget.spent_amounts[category],
            'allocated_amount': budget.allocated_amounts[category],
            'percentage_used': (budget.spent_amounts[category] / budget.allocated_amounts[category]) * 100,
            'alerts': alerts
        }
    
    def get_budget_summary(self, project_id: str) -> Dict[str, Any]:
        """Get comprehensive budget summary for a project."""
        budgets = self.load_budgets()
        
        if project_id not in budgets:
            raise ValueError(f"Project {project_id} not found")
        
        budget = budgets[project_id]
        
        total_spent = sum(budget.spent_amounts.values())
        total_allocated = sum(budget.allocated_amounts.values())
        
        category_summaries = []
        for category in budget.allocated_amounts.keys():
            allocated = budget.allocated_amounts[category]
            spent = budget.spent_amounts[category]
            percentage = (spent / allocated) * 100 if allocated > 0 else 0
            
            status = 'on_track'
            if percentage > 100:
                status = 'overrun'
            elif percentage > 90:
                status = 'critical'
            elif percentage > 80:
                status = 'warning'
            
            category_summaries.append({
                'category': category,
                'allocated': allocated,
                'spent': spent,
                'remaining': allocated - spent,
                'percentage_used': round(percentage, 2),
                'status': status
            })
        
        return {
            'project_id': project_id,
            'total_budget': budget.total_budget,
            'total_allocated': total_allocated,
            'total_spent': total_spent,
            'total_remaining': total_allocated - total_spent,
            'overall_percentage_used': round((total_spent / total_allocated) * 100, 2),
            'categories': category_summaries,
            'last_updated': budget.last_updated
        }
    
    def _check_budget_alerts(self, budget: Budget, category: str) -> List[BudgetAlert]:
        """Check for budget threshold alerts."""
        alerts = []
        
        allocated = budget.allocated_amounts[category]
        spent = budget.spent_amounts[category]
        percentage = (spent / allocated) * 100 if allocated > 0 else 0
        
        alert_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        
        if percentage > 100:
            alerts.append(BudgetAlert(
                alert_id=alert_id,
                project_id=budget.project_id,
                category=category,
                alert_type='overrun',
                message=f'Budget overrun in {category}: {percentage:.1f}% used',
                percentage_used=percentage,
                created_date=now
            ))
        elif percentage > 90:
            alerts.append(BudgetAlert(
                alert_id=alert_id,
                project_id=budget.project_id,
                category=category,
                alert_type='critical',
                message=f'Critical budget usage in {category}: {percentage:.1f}% used',
                percentage_used=percentage,
                created_date=now
            ))
        elif percentage > 80:
            alerts.append(BudgetAlert(
                alert_id=alert_id,
                project_id=budget.project_id,
                category=category,
                alert_type='warning',
                message=f'Budget warning in {category}: {percentage:.1f}% used',
                percentage_used=percentage,
                created_date=now
            ))
        
        # Save alerts
        if alerts:
            self._save_alerts(alerts)
        
        return alerts
    
    def _save_alerts(self, new_alerts: List[BudgetAlert]):
        """Save budget alerts to storage."""
        existing_alerts = []
        if self.alerts_file.exists():
            with open(self.alerts_file, 'r') as f:
                existing_data = json.load(f)
                existing_alerts = [BudgetAlert(**alert) for alert in existing_data]
        
        all_alerts = existing_alerts + new_alerts
        
        # Keep only last 100 alerts
        all_alerts = all_alerts[-100:]
        
        alerts_data = []
        for alert in all_alerts:
            alerts_data.append({
                'alert_id': alert.alert_id,
                'project_id': alert.project_id,
                'category': alert.category,
                'alert_type': alert.alert_type,
                'message': alert.message,
                'percentage_used': alert.percentage_used,
                'created_date': alert.created_date
            })
        
        with open(self.alerts_file, 'w') as f:
            json.dump(alerts_data, f, indent=2)
    
    def get_project_alerts(self, project_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get recent alerts for a project."""
        if not self.alerts_file.exists():
            return []
        
        with open(self.alerts_file, 'r') as f:
            alerts_data = json.load(f)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        project_alerts = []
        for alert_data in alerts_data:
            if alert_data['project_id'] == project_id:
                alert_date = datetime.fromisoformat(alert_data['created_date'])
                if alert_date >= cutoff_date:
                    project_alerts.append(alert_data)
        
        return sorted(project_alerts, key=lambda x: x['created_date'], reverse=True)

# Utility functions for common budget categories in construction
def get_default_construction_categories() -> Dict[str, float]:
    """Get default construction budget categories with suggested percentages."""
    return {
        'materials': 0.45,      # 45% of budget
        'labor': 0.25,          # 25% of budget
        'equipment': 0.15,      # 15% of budget
        'overhead': 0.10,       # 10% of budget
        'contingency': 0.05     # 5% of budget
    }

def create_construction_budget(project_id: str, total_budget: float, 
                             custom_percentages: Optional[Dict[str, float]] = None) -> Budget:
    """Helper function to create a construction project budget."""
    tracker = BudgetTracker()
    
    # Use custom percentages or defaults
    percentages = custom_percentages or get_default_construction_categories()
    
    # Convert percentages to amounts
    allocations = {}
    for category, percentage in percentages.items():
        allocations[category] = total_budget * percentage
    
    return tracker.create_budget(project_id, total_budget, allocations)