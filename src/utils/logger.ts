interface LogData {
  [key: string]: any;
}

class Logger {
  private formatMessage(level: string, message: string, data?: LogData): string {
    const timestamp = new Date().toISOString();
    const logObject = {
      timestamp,
      level,
      message,
      service: 'xxx-task',
      ...data
    };
    return JSON.stringify(logObject);
  }

  info(message: string, data?: LogData): void {
    console.log(this.formatMessage('INFO', message, data));
  }

  error(message: string, data?: LogData): void {
    console.error(this.formatMessage('ERROR', message, data));
  }

  warn(message: string, data?: LogData): void {
    console.warn(this.formatMessage('WARN', message, data));
  }

  debug(message: string, data?: LogData): void {
    if (process.env.NODE_ENV === 'development') {
      console.debug(this.formatMessage('DEBUG', message, data));
    }
  }
}

export const logger = new Logger();