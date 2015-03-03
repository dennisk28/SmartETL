package org.f3tools.incredible.smartETL;

public class Flow {
	private Step from;
	public Step getFrom() {
		return from;
	}
	public void setFrom(Step from) {
		this.from = from;
	}
	public Step getTo() {
		return to;
	}
	public void setTo(Step to) {
		this.to = to;
	}
	private Step to;

}
