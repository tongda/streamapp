package com.thoughtworks.injestion;

import com.thoughtworks.message.MessageProtos;

import java.io.Serializable;

/**
 * Created by dtong on 20/07/2017.
 */
public class CompletionChecker implements Serializable {
    private int numTrades;
    private int numMarketData;

    public CompletionChecker(int numTrades, int numMarketData) {
        this.numTrades = numTrades;
        this.numMarketData = numMarketData;
    }

    public int getNumTrades() {
        return numTrades;
    }

    public void setNumTrades(int numTrades) {
        this.numTrades = numTrades;
    }

    public int getNumMarketData() {
        return numMarketData;
    }

    public void setNumMarketData(int numMarketData) {
        this.numMarketData = numMarketData;
    }

    public boolean isCompleted() {
        return numTrades == 0 && numMarketData == 0;
    }

    public CompletionChecker substract(CompletionChecker other) {
        return new CompletionChecker(
                this.numTrades - other.numTrades,
                this.numMarketData - other.numMarketData);
    }

    public CompletionChecker consume(MessageProtos.DataType type) {
        switch (type) {
            case TRADE_DATA:
                return new CompletionChecker(this.numTrades + 1, this.numMarketData);
            case MARKET_DATA:
                return new CompletionChecker(this.numTrades, this.numMarketData + 1);
            default:
                return this;
        }
    }

    @Override
    public String toString() {
        return "num trade: " + numTrades + "; num market data: " + numMarketData;
    }

    public static CompletionChecker empty() {
        return new CompletionChecker(0, 0);
    }
}
