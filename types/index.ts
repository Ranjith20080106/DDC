export interface User {
  id: string;
  email: string;
  passwordHash?: string;
  createdAt: Date;
}

export interface CodeGeneration {
  id: string;
  userId: string;
  prompt: string;
  generatedCode: string;
  createdAt: Date;
}

export interface JWTPayload {
  userId: string;
  email: string;
}

export interface ApiResponse<T = any> {
  success: boolean;
  message?: string;
  data?: T;
  error?: string;
}

export interface AuthResponse {
  token: string;
  user: {
    id: string;
    email: string;
  };
}
