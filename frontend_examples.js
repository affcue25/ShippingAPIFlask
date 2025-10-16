// Frontend JavaScript Examples for Shipping Data API
// These examples show how to integrate with the API from a web frontend

const API_BASE_URL = 'http://localhost:5000/api';

// Example 1: Fetch all shipments with pagination
async function fetchShipments(page = 1, limit = 10, dateFilter = null) {
    try {
        const params = new URLSearchParams({
            page: page.toString(),
            limit: limit.toString()
        });
        
        if (dateFilter) {
            params.append('date_filter', dateFilter);
        }
        
        const response = await fetch(`${API_BASE_URL}/shipments?${params}`);
        const data = await response.json();
        
        console.log('Shipments:', data.data);
        console.log('Pagination:', data.pagination);
        
        return data;
    } catch (error) {
        console.error('Error fetching shipments:', error);
    }
}

// Example 2: Filter shipments by city
async function filterShipmentsByCity(city, dateFilter = null) {
    try {
        const params = new URLSearchParams({
            column: 'shipper_city',
            value: city
        });
        
        if (dateFilter) {
            params.append('date_filter', dateFilter);
        }
        
        const response = await fetch(`${API_BASE_URL}/shipments/filter?${params}`);
        const data = await response.json();
        
        console.log(`Shipments from ${city}:`, data.data);
        return data;
    } catch (error) {
        console.error('Error filtering shipments:', error);
    }
}

// Example 3: Get top customers
async function getTopCustomers(dateFilter = 'month', limit = 10) {
    try {
        const params = new URLSearchParams({
            date_filter: dateFilter,
            limit: limit.toString()
        });
        
        const response = await fetch(`${API_BASE_URL}/customers/top?${params}`);
        const data = await response.json();
        
        console.log('Top customers:', data.data);
        return data;
    } catch (error) {
        console.error('Error fetching top customers:', error);
    }
}

// Example 4: Advanced search
async function advancedSearch(fromCity = null, toCity = null, dateFilter = null) {
    try {
        const params = new URLSearchParams();
        
        if (fromCity) params.append('from_city', fromCity);
        if (toCity) params.append('to_city', toCity);
        if (dateFilter) params.append('date_filter', dateFilter);
        
        const response = await fetch(`${API_BASE_URL}/shipments/advanced-search?${params}`);
        const data = await response.json();
        
        console.log('Advanced search results:', data.data);
        return data;
    } catch (error) {
        console.error('Error in advanced search:', error);
    }
}

// Example 5: Export data to CSV
async function exportToCSV(shipmentData) {
    try {
        const response = await fetch(`${API_BASE_URL}/export`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                format: 'csv',
                data: shipmentData
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            // Create download link
            const downloadUrl = `${API_BASE_URL}${result.download_url}`;
            const link = document.createElement('a');
            link.href = downloadUrl;
            link.download = result.filename;
            link.click();
            
            console.log('Export successful:', result);
        }
        
        return result;
    } catch (error) {
        console.error('Error exporting data:', error);
    }
}

// Example 6: Get analytics data
async function getAnalytics(dateFilter = 'month') {
    try {
        const [totalShipments, averageWeight, topCities] = await Promise.all([
            fetch(`${API_BASE_URL}/shipments/total?date_filter=${dateFilter}`).then(r => r.json()),
            fetch(`${API_BASE_URL}/shipments/average-weight?date_filter=${dateFilter}`).then(r => r.json()),
            fetch(`${API_BASE_URL}/cities/top?date_filter=${dateFilter}&limit=5`).then(r => r.json())
        ]);
        
        const analytics = {
            totalShipments: totalShipments.data.total,
            averageWeight: averageWeight.data.average_weight,
            topCities: topCities.data
        };
        
        console.log('Analytics data:', analytics);
        return analytics;
    } catch (error) {
        console.error('Error fetching analytics:', error);
    }
}

// Example 7: Search by weight
async function searchByWeight(minWeight, dateFilter = null) {
    try {
        const params = new URLSearchParams({
            min_weight: minWeight.toString()
        });
        
        if (dateFilter) {
            params.append('date_filter', dateFilter);
        }
        
        const response = await fetch(`${API_BASE_URL}/shipments/by-weight?${params}`);
        const data = await response.json();
        
        console.log(`Shipments weighing more than ${minWeight}kg:`, data.data);
        return data;
    } catch (error) {
        console.error('Error searching by weight:', error);
    }
}

// Example 8: Get shipments by shipper
async function getShipmentsByShipper(shipperName, dateFilter = null) {
    try {
        const params = new URLSearchParams({
            shipper_name: shipperName
        });
        
        if (dateFilter) {
            params.append('date_filter', dateFilter);
        }
        
        const response = await fetch(`${API_BASE_URL}/shipments/by-shipper?${params}`);
        const data = await response.json();
        
        console.log(`Shipments for ${shipperName}:`, data.data);
        return data;
    } catch (error) {
        console.error('Error fetching shipments by shipper:', error);
    }
}

// Usage Examples:
// fetchShipments(1, 20, 'month');
// filterShipmentsByCity('Dubai', 'week');
// getTopCustomers('year', 5);
// advancedSearch('Dubai', 'Abu Dhabi', 'month');
// searchByWeight(5.0, 'month');
// getShipmentsByShipper('ABC Company', 'month');
// getAnalytics('week');
