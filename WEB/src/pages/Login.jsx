import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Mail, Lock, LogIn, AlertCircle } from 'lucide-react';
import { useAuth } from '../context/AuthContext';

function Login() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isSignUp, setIsSignUp] = useState(false);
  const [confirmPassword, setConfirmPassword] = useState('');
  const navigate = useNavigate();
  const { login } = useAuth();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setIsLoading(true);

    try {
      // Validation
      if (!email || !password) {
        setError('Vui lòng điền đầy đủ thông tin');
        setIsLoading(false);
        return;
      }

      if (!/\S+@\S+\.\S+/.test(email)) {
        setError('Email không hợp lệ');
        setIsLoading(false);
        return;
      }

      if (isSignUp) {
        if (password !== confirmPassword) {
          setError('Mật khẩu không khớp');
          setIsLoading(false);
          return;
        }
        if (password.length < 6) {
          setError('Mật khẩu phải tối thiểu 6 ký tự');
          setIsLoading(false);
          return;
        }
      }

      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1500));

      // Demo login/signup
      if (email && password.length >= 6) {
        login({
          email,
          name: email.split('@')[0],
          loginTime: new Date().toLocaleString('vi-VN'),
        });
        navigate('/');
      } else {
        setError('Thông tin không hợp lệ');
      }
    } catch (err) {
      setError('Đã xảy ra lỗi. Vui lòng thử lại.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center p-4 relative overflow-hidden"
         style={{
           backgroundImage: 'linear-gradient(135deg, rgba(30,70,120,0.7) 0%, rgba(20,90,180,0.7) 100%), url("https://images.unsplash.com/photo-1552664730-d307ca884978?w=1500&h=1500&fit=crop")',
           backgroundSize: 'cover',
           backgroundPosition: 'center',
           backgroundAttachment: 'fixed',
           backgroundRepeat: 'no-repeat'
         }}>
      {/* Decorative elements */}
      <div className="absolute top-0 left-0 w-72 h-72 bg-walmart-yellow opacity-5 rounded-full -translate-x-1/2 -translate-y-1/2"></div>
      <div className="absolute bottom-0 right-0 w-96 h-96 bg-walmart-blue opacity-5 rounded-full translate-x-1/2 translate-y-1/2"></div>

      {/* Login Container */}
      <div className="relative w-full max-w-md z-10">
        <div className="bg-white rounded-2xl shadow-2xl p-8">
          {/* Header */}
          <div className="text-center mb-8">
            <div className="inline-flex items-center justify-center w-16 h-16 bg-gradient-to-br from-walmart-blue to-blue-600 rounded-full mb-4">
              <span className="text-2xl font-bold text-white">W</span>
            </div>
            <h1 className="text-3xl font-bold text-gray-900 mb-2">Walmart Analytics</h1>
            <p className="text-gray-600">{isSignUp ? 'Tạo tài khoản mới' : 'Dashboard Quản lý dữ liệu bán hàng'}</p>
          </div>

          {/* Error Message */}
          {error && (
            <div className="mb-6 p-4 bg-red-50 border-l-4 border-red-500 rounded flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
              <p className="text-sm text-red-700">{error}</p>
            </div>
          )}

          {/* Form */}
          <form onSubmit={handleSubmit} className="space-y-5">
            {/* Email Input */}
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-2">
                Email
              </label>
              <div className="relative">
                <Mail className="absolute left-3 top-3 w-5 h-5 text-gray-400" />
                <input
                  id="email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="your@email.com"
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-walmart-blue focus:border-transparent transition"
                  disabled={isLoading}
                />
              </div>
            </div>

            {/* Password Input */}
            <div>
              <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-2">
                Mật khẩu
              </label>
              <div className="relative">
                <Lock className="absolute left-3 top-3 w-5 h-5 text-gray-400" />
                <input
                  id="password"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-walmart-blue focus:border-transparent transition"
                  disabled={isLoading}
                />
              </div>
              <p className="text-xs text-gray-500 mt-1">Tối thiểu 6 ký tự</p>
            </div>

            {/* Confirm Password Input (chỉ hiển thị khi signup) */}
            {isSignUp && (
              <div>
                <label htmlFor="confirmPassword" className="block text-sm font-medium text-gray-700 mb-2">
                  Xác nhận mật khẩu
                </label>
                <div className="relative">
                  <Lock className="absolute left-3 top-3 w-5 h-5 text-gray-400" />
                  <input
                    id="confirmPassword"
                    type="password"
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                    placeholder="••••••••"
                    className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-walmart-blue focus:border-transparent transition"
                    disabled={isLoading}
                  />
                </div>
              </div>
            )}

            {/* Remember & Forgot */}
            {!isSignUp && (
              <div className="flex items-center justify-between text-sm">
                <label className="flex items-center">
                  <input type="checkbox" className="w-4 h-4 text-walmart-blue rounded" />
                  <span className="ml-2 text-gray-600">Nhớ tôi</span>
                </label>
                <a href="#" className="text-walmart-blue hover:underline font-medium">
                  Quên mật khẩu?
                </a>
              </div>
            )}

            {/* Login/SignUp Button */}
            <button
              type="submit"
              disabled={isLoading}
              className="w-full bg-gradient-to-r from-walmart-blue to-blue-600 text-white font-semibold py-2.5 rounded-lg hover:shadow-lg hover:scale-105 transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
            >
              {isLoading ? (
                <>
                  <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                  {isSignUp ? 'Đang tạo tài khoản...' : 'Đang đăng nhập...'}
                </>
              ) : (
                <>
                  <LogIn className="w-5 h-5" />
                  {isSignUp ? 'Tạo tài khoản' : 'Đăng nhập'}
                </>
              )}
            </button>

            {/* Toggle Sign Up/Login */}
            <button
              type="button"
              onClick={() => {
                setIsSignUp(!isSignUp);
                setEmail('');
                setPassword('');
                setConfirmPassword('');
                setError('');
              }}
              className="w-full border-2 border-gray-300 text-gray-700 font-semibold py-2.5 rounded-lg hover:border-walmart-blue hover:bg-blue-50 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isSignUp ? 'Đã có tài khoản? Đăng nhập' : 'Chưa có tài khoản? Tạo mới'}
            </button>
          </form>

          {/* Footer */}
        </div>

        {/* Info Box */}
        <div className="mt-6 p-4 bg-walmart-blue bg-opacity-90 rounded-lg text-white text-sm backdrop-blur z-10 relative">
          <p className="font-semibold mb-2">ℹ️ Thông tin:</p>
          <ul className="space-y-1 text-white text-opacity-95">
            <li>• Email: phải là email hợp lệ</li>
            <li>• Mật khẩu: tối thiểu 6 ký tự</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

export default Login;
