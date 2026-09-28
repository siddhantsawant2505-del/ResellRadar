/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: 'class',
  content: [
    './app/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        background: '#13110f',
        surface: {
          DEFAULT: '#13110f',
          lowest: '#0e0d0b',
          low: '#1c1a17',
          container: '#252320',
          high: '#2e2c28',
          highest: '#3a3834',
        },
        primary: {
          DEFAULT: '#e8a733',
          container: '#c48820',
          dim: '#f5c86a',
        },
        secondary: {
          DEFAULT: '#5cb87a',
          container: '#3a9957',
        },
        tertiary: {
          DEFAULT: '#9b8ff0',
          container: '#6b5fd4',
        },
        accent: {
          amber: '#c8892a',
          crimson: '#d9534f',
        },
        foreground: '#d4cfc9',
        outline: {
          DEFAULT: '#a09890',
          variant: '#5c5450',
        },
      },
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
    },
  },
  plugins: [],
}
