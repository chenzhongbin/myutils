package our.db.sync.support;

import java.util.ArrayList;
import java.util.Collection;

import our.db.sync.Synchronizer;
import our.db.sync.listener.SynchronizeListener;

public abstract class AbstractSynchronizer implements Synchronizer {

	private Collection<SynchronizeListener> list = new ArrayList<SynchronizeListener>();

	public void addListener(SynchronizeListener listener) {
		list.add(listener);
	}

	public void removeListener(SynchronizeListener listener) {
		list.remove(listener);
	}

}
