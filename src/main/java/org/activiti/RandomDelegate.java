package org.activiti;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;

import java.util.Random;

public class RandomDelegate implements JavaDelegate {

    private static Random random = new Random();

    public void execute(DelegateExecution delegateExecution) {
        Number number1 = (Number) delegateExecution.getVariable("input1");
        Number number2 = (Number) delegateExecution.getVariable("input2");
        int result = number1.intValue() + number2.intValue();
        delegateExecution.setVariable("result_" + random.nextInt(), "result is " + result);
    }

}
