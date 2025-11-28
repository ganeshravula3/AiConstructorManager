"""
Vendor Analytics and Performance Tracking Module

Provides comprehensive vendor analysis, performance scoring, and risk assessment.
"""
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import statistics

@dataclass
class VendorTransaction:
    transaction_id: str
    vendor_name: str
    project_id: str
    amount: float
    transaction_date: str
    payment_date: Optional[str]
    category: str
    status: str  # 'pending', 'paid', 'overdue', 'disputed'
    quality_rating: Optional[int]  # 1-5 scale
    delivery_rating: Optional[int]  # 1-5 scale
    notes: str

@dataclass
class VendorPerformance:
    vendor_name: str
    total_transactions: int
    total_amount: float
    average_transaction: float
    on_time_payment_rate: float
    average_quality_rating: float
    average_delivery_rating: float
    risk_score: float
    last_transaction_date: str
    projects_worked: List[str]

class VendorAnalytics:
    def __init__(self, storage_path: str = "backend/storage"):
        self.storage_path = Path(storage_path)
        self.transactions_file = self.storage_path / "vendor_transactions.json"
        self.performance_file = self.storage_path / "vendor_performance.json"
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize with sample data if files don't exist
        self._initialize_sample_data()
    
    def _initialize_sample_data(self):
        """Initialize with realistic sample vendor data."""
        if not self.transactions_file.exists():
            sample_transactions = self._create_sample_transactions()
            self._save_transactions(sample_transactions)
        
        # Update performance metrics
        self.update_all_vendor_performance()
    
    def _create_sample_transactions(self) -> List[VendorTransaction]:
        """Create realistic sample vendor transactions."""
        vendors = [
            "ABC Construction Supplies",
            "Metro Steel Corporation", 
            "Reliable Cement Co.",
            "Quick Delivery Services",
            "Premium Building Materials",
            "City Hardware Store",
            "Industrial Equipment Rentals",
            "Local Labor Contractors"
        ]
        
        projects = ["proj-001", "proj-002", "proj-003", "proj-004"]
        categories = ["materials", "labor", "equipment", "services"]
        
        transactions = []
        base_date = datetime.now() - timedelta(days=365)
        
        for i in range(150):  # 150 sample transactions
            vendor = vendors[i % len(vendors)]
            project = projects[i % len(projects)]
            category = categories[i % len(categories)]
            
            # Generate realistic transaction
            amount = 15000 + (i * 1000) + (hash(vendor) % 50000)
            trans_date = base_date + timedelta(days=i * 2)
            
            # Some transactions are paid, some pending
            status = 'paid' if i % 3 == 0 else 'pending' if i % 3 == 1 else 'overdue'
            payment_date = trans_date + timedelta(days=15) if status == 'paid' else None
            
            # Performance ratings (some vendors better than others)
            base_quality = 4 if 'Premium' in vendor or 'Reliable' in vendor else 3
            quality_rating = min(5, max(1, base_quality + (hash(vendor + str(i)) % 3) - 1))
            delivery_rating = min(5, max(1, base_quality + (hash(vendor + str(i*2)) % 3) - 1))
            
            transaction = VendorTransaction(
                transaction_id=f"txn-{uuid.uuid4().hex[:8]}",
                vendor_name=vendor,
                project_id=project,
                amount=amount,
                transaction_date=trans_date.isoformat(),
                payment_date=payment_date.isoformat() if payment_date else None,
                category=category,
                status=status,
                quality_rating=quality_rating,
                delivery_rating=delivery_rating,
                notes=f"Sample transaction for {category}"
            )
            transactions.append(transaction)
        
        return transactions
    
    def _save_transactions(self, transactions: List[VendorTransaction]):
        """Save transactions to storage."""
        data = []
        for txn in transactions:
            data.append({
                'transaction_id': txn.transaction_id,
                'vendor_name': txn.vendor_name,
                'project_id': txn.project_id,
                'amount': txn.amount,
                'transaction_date': txn.transaction_date,
                'payment_date': txn.payment_date,
                'category': txn.category,
                'status': txn.status,
                'quality_rating': txn.quality_rating,
                'delivery_rating': txn.delivery_rating,
                'notes': txn.notes
            })
        
        with open(self.transactions_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def load_transactions(self) -> List[VendorTransaction]:
        """Load all vendor transactions."""
        if not self.transactions_file.exists():
            return []
        
        with open(self.transactions_file, 'r') as f:
            data = json.load(f)
        
        transactions = []
        for txn_data in data:
            transactions.append(VendorTransaction(**txn_data))
        
        return transactions
    
    def add_transaction(self, vendor_name: str, project_id: str, amount: float,
                       category: str, quality_rating: int = 3, 
                       delivery_rating: int = 3, notes: str = "") -> str:
        """Add a new vendor transaction."""
        transaction = VendorTransaction(
            transaction_id=f"txn-{uuid.uuid4().hex[:8]}",
            vendor_name=vendor_name,
            project_id=project_id,
            amount=amount,
            transaction_date=datetime.now().isoformat(),
            payment_date=None,
            category=category,
            status='pending',
            quality_rating=quality_rating,
            delivery_rating=delivery_rating,
            notes=notes
        )
        
        transactions = self.load_transactions()
        transactions.append(transaction)
        self._save_transactions(transactions)
        
        # Update vendor performance
        self.update_vendor_performance(vendor_name)
        
        return transaction.transaction_id
    
    def mark_payment(self, transaction_id: str, payment_date: Optional[str] = None) -> bool:
        """Mark a transaction as paid."""
        transactions = self.load_transactions()
        
        for txn in transactions:
            if txn.transaction_id == transaction_id:
                txn.status = 'paid'
                txn.payment_date = payment_date or datetime.now().isoformat()
                self._save_transactions(transactions)
                
                # Update vendor performance
                self.update_vendor_performance(txn.vendor_name)
                return True
        
        return False
    
    def get_vendor_performance(self, vendor_name: str) -> Dict[str, Any]:
        """Get comprehensive performance analysis for a vendor."""
        transactions = self.load_transactions()
        vendor_txns = [txn for txn in transactions if txn.vendor_name == vendor_name]
        
        if not vendor_txns:
            return {'error': f'No transactions found for vendor: {vendor_name}'}
        
        # Calculate performance metrics
        total_amount = sum(txn.amount for txn in vendor_txns)
        avg_transaction = total_amount / len(vendor_txns)
        
        # Payment performance
        paid_txns = [txn for txn in vendor_txns if txn.status == 'paid' and txn.payment_date]
        on_time_payments = 0
        
        for txn in paid_txns:
            trans_date = datetime.fromisoformat(txn.transaction_date)
            pay_date = datetime.fromisoformat(txn.payment_date)
            if (pay_date - trans_date).days <= 30:  # Consider 30 days as on-time
                on_time_payments += 1
        
        on_time_rate = (on_time_payments / len(paid_txns)) * 100 if paid_txns else 0
        
        # Quality metrics
        quality_ratings = [txn.quality_rating for txn in vendor_txns if txn.quality_rating]
        delivery_ratings = [txn.delivery_rating for txn in vendor_txns if txn.delivery_rating]
        
        avg_quality = statistics.mean(quality_ratings) if quality_ratings else 0
        avg_delivery = statistics.mean(delivery_ratings) if delivery_ratings else 0
        
        # Risk assessment
        overdue_txns = len([txn for txn in vendor_txns if txn.status == 'overdue'])
        disputed_txns = len([txn for txn in vendor_txns if txn.status == 'disputed'])
        
        risk_score = self._calculate_risk_score(
            on_time_rate, avg_quality, avg_delivery, 
            overdue_txns, disputed_txns, len(vendor_txns)
        )
        
        # Projects worked on
        projects = list(set(txn.project_id for txn in vendor_txns))
        
        return {
            'vendor_name': vendor_name,
            'total_transactions': len(vendor_txns),
            'total_amount': round(total_amount, 2),
            'average_transaction': round(avg_transaction, 2),
            'on_time_payment_rate': round(on_time_rate, 1),
            'average_quality_rating': round(avg_quality, 2),
            'average_delivery_rating': round(avg_delivery, 2),
            'risk_score': risk_score,
            'risk_level': self._get_risk_level(risk_score),
            'overdue_transactions': overdue_txns,
            'disputed_transactions': disputed_txns,
            'projects_worked': projects,
            'last_transaction_date': max(txn.transaction_date for txn in vendor_txns),
            'status_breakdown': self._get_status_breakdown(vendor_txns)
        }
    
    def _calculate_risk_score(self, on_time_rate: float, quality: float, 
                            delivery: float, overdue: int, disputed: int, 
                            total: int) -> float:
        """Calculate vendor risk score (0-100, lower is better)."""
        # Base score from performance ratings
        performance_score = ((5 - quality) + (5 - delivery)) / 2 * 20  # 0-40 points
        
        # Payment reliability (0-30 points)
        payment_score = (100 - on_time_rate) * 0.3
        
        # Issue frequency (0-30 points)
        issue_rate = ((overdue + disputed) / total) * 100 if total > 0 else 0
        issue_score = min(30, issue_rate)
        
        total_score = performance_score + payment_score + issue_score
        return round(min(100, total_score), 1)
    
    def _get_risk_level(self, risk_score: float) -> str:
        """Convert risk score to risk level."""
        if risk_score < 20:
            return 'low'
        elif risk_score < 40:
            return 'moderate'
        elif risk_score < 60:
            return 'high'
        else:
            return 'very_high'
    
    def _get_status_breakdown(self, transactions: List[VendorTransaction]) -> Dict[str, int]:
        """Get breakdown of transaction statuses."""
        breakdown = {}
        for txn in transactions:
            breakdown[txn.status] = breakdown.get(txn.status, 0) + 1
        return breakdown
    
    def update_vendor_performance(self, vendor_name: str):
        """Update cached performance metrics for a vendor."""
        performance = self.get_vendor_performance(vendor_name)
        
        # Save to performance file
        all_performance = self.load_all_performance()
        all_performance[vendor_name] = performance
        
        with open(self.performance_file, 'w') as f:
            json.dump(all_performance, f, indent=2)
    
    def update_all_vendor_performance(self):
        """Update performance metrics for all vendors."""
        transactions = self.load_transactions()
        vendors = set(txn.vendor_name for txn in transactions)
        
        for vendor in vendors:
            self.update_vendor_performance(vendor)
    
    def load_all_performance(self) -> Dict[str, Dict[str, Any]]:
        """Load all vendor performance data."""
        if not self.performance_file.exists():
            return {}
        
        with open(self.performance_file, 'r') as f:
            return json.load(f)
    
    def get_top_vendors(self, limit: int = 10, sort_by: str = 'performance') -> List[Dict[str, Any]]:
        """Get top performing vendors."""
        all_performance = self.load_all_performance()
        
        vendors = list(all_performance.values())
        
        if sort_by == 'performance':
            # Sort by combination of quality and low risk
            vendors.sort(key=lambda x: (x['average_quality_rating'] + x['average_delivery_rating']) - x['risk_score']/20, reverse=True)
        elif sort_by == 'volume':
            vendors.sort(key=lambda x: x['total_amount'], reverse=True)
        elif sort_by == 'reliability':
            vendors.sort(key=lambda x: x['on_time_payment_rate'], reverse=True)
        
        return vendors[:limit]
    
    def get_vendor_recommendations(self, category: str, project_budget: float) -> List[Dict[str, Any]]:
        """Get vendor recommendations for a category and budget."""
        transactions = self.load_transactions()
        category_vendors = {}
        
        # Group vendors by category
        for txn in transactions:
            if txn.category == category:
                if txn.vendor_name not in category_vendors:
                    category_vendors[txn.vendor_name] = []
                category_vendors[txn.vendor_name].append(txn)
        
        recommendations = []
        for vendor_name, vendor_txns in category_vendors.items():
            performance = self.get_vendor_performance(vendor_name)
            
            # Check if vendor works in similar budget ranges
            avg_transaction = performance['average_transaction']
            budget_fit = 1.0 - abs(avg_transaction - project_budget) / max(avg_transaction, project_budget)
            
            # Calculate recommendation score
            score = (
                performance['average_quality_rating'] * 0.3 +
                performance['average_delivery_rating'] * 0.3 +
                (100 - performance['risk_score']) * 0.002 +
                performance['on_time_payment_rate'] * 0.002 +
                budget_fit * 20
            )
            
            recommendations.append({
                'vendor_name': vendor_name,
                'recommendation_score': round(score, 1),
                'performance_summary': performance,
                'budget_fit': round(budget_fit * 100, 1),
                'reason': self._get_recommendation_reason(performance, budget_fit)
            })
        
        # Sort by recommendation score
        recommendations.sort(key=lambda x: x['recommendation_score'], reverse=True)
        
        return recommendations[:5]  # Return top 5 recommendations
    
    def _get_recommendation_reason(self, performance: Dict[str, Any], budget_fit: float) -> str:
        """Generate recommendation reason."""
        reasons = []
        
        if performance['average_quality_rating'] >= 4:
            reasons.append("excellent quality rating")
        if performance['average_delivery_rating'] >= 4:
            reasons.append("reliable delivery")
        if performance['risk_score'] < 30:
            reasons.append("low risk profile")
        if performance['on_time_payment_rate'] > 80:
            reasons.append("good payment history")
        if budget_fit > 0.8:
            reasons.append("good budget fit")
        
        if not reasons:
            reasons.append("meets basic requirements")
        
        return ", ".join(reasons)