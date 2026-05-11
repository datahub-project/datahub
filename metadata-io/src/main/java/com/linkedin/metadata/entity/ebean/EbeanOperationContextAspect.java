package com.linkedin.metadata.entity.ebean;

import io.datahubproject.metadata.context.OperationContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class EbeanOperationContextAspect {

  @Around("execution(public * com.linkedin.metadata.entity.ebean.EbeanAspectDao.*(..))")
  public Object aroundEbeanDaoMethod(ProceedingJoinPoint pjp) throws Throwable {
    System.out.println(" ASPECT FIRED for: " + pjp.getSignature());
    OperationContext opContext = findOperationContext(pjp.getArgs());

    if (opContext == null) {
      return pjp.proceed();
    }

    OperationContext previous = EbeanAspectDao.currentOperationContext();

    try {
      EbeanAspectDao.setCurrentOperationContext(opContext);
      return pjp.proceed();
    } finally {
      if (previous != null) {
        EbeanAspectDao.setCurrentOperationContext(previous);
      } else {
        EbeanAspectDao.clearCurrentOperationContext();
      }
    }
  }

  private OperationContext findOperationContext(Object[] args) {
    for (Object arg : args) {
      if (arg instanceof OperationContext) {
        return (OperationContext) arg;
      }
    }
    return null;
  }
}
