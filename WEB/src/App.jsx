import React, { useState } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Bot, Sparkles } from 'lucide-react';
import Header from './components/Layout/Header';
import Sidebar from './components/Layout/Sidebar';
import SalesPerformance from './components/Dashboards/SalesPerformance';
import StorePerformance from './components/Dashboards/StorePerformance';
import EcommerceDashboard from './components/Dashboards/EcommerceDashboard';
import CustomerAnalytics from './components/Dashboards/CustomerAnalytics';
import ChatBot from './components/ChatBot/ChatBot';
import Login from './pages/Login';
import ProtectedRoute from './components/ProtectedRoute';
import { AuthProvider, useAuth } from './context/AuthContext';

function AppContent() {
  const [currentDashboard, setCurrentDashboard] = useState('Sales Performance');
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [isChatMinimized, setIsChatMinimized] = useState(false);
  const { isLoggedIn } = useAuth();

  const toggleChat = () => {
    if (isChatMinimized) {
      setIsChatMinimized(false);
    } else {
      setIsChatOpen(!isChatOpen);
    }
  };

  // Nếu chưa login, hiển thị login page
  if (!isLoggedIn) {
    return (
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="*" element={<Login />} />
      </Routes>
    );
  }

  // Nếu đã login, hiển thị dashboard
  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <Sidebar currentDashboard={currentDashboard} setCurrentDashboard={setCurrentDashboard} />
      
      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Header */}
        <Header />
        
        {/* Dashboard Content */}
        <main className="flex-1 overflow-y-auto p-6">
          <Routes>
            <Route path="/" element={<SalesPerformance />} />
            <Route path="/sales" element={<SalesPerformance />} />
            <Route path="/store" element={<StorePerformance />} />
            <Route path="/ecommerce" element={<EcommerceDashboard />} />
            <Route path="/customer" element={<CustomerAnalytics />} />
          </Routes>
        </main>
      </div>

      {/* Chat Toggle Button */}
      {!isChatOpen && (
        <button
          onClick={toggleChat}
          className="fixed bottom-6 right-6 bg-gradient-to-r from-walmart-blue to-blue-600 text-white p-4 rounded-full shadow-2xl hover:shadow-xl hover:scale-105 transition-all z-40 group"
        >
          <div className="flex items-center gap-2">
            <Bot className="w-6 h-6" />
            <span className="hidden group-hover:inline font-medium pr-1">Ask Alyss</span>
            <Sparkles className="w-4 h-4 text-walmart-yellow" />
          </div>
        </button>
      )}

      {/* AI Chatbot */}
      <ChatBot 
        isOpen={isChatOpen} 
        onClose={() => setIsChatOpen(false)}
        isMinimized={isChatMinimized}
        onMinimize={() => setIsChatMinimized(!isChatMinimized)}
      />
    </div>
  );
}

function App() {
  return (
    <Router>
      <AuthProvider>
        <AppContent />
      </AuthProvider>
    </Router>
  );
}

export default App;
