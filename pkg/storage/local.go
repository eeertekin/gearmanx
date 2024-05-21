package storage

import "sync"

type LocalStorage struct {
	mutex sync.RWMutex
	db    map[string]any
}

func NewLocalStorage() *LocalStorage {
	return &LocalStorage{
		db: make(map[string]any),
	}
}

func (l *LocalStorage) Get(key string) any {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.db[key]
}

func (l *LocalStorage) Len() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return len(l.db)
}

func (l *LocalStorage) Delete(key string) any {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	delete(l.db, key)
	return nil
}

func (l *LocalStorage) Set(key string, value any) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.db[key] = value
}

func (l *LocalStorage) IsSet(key string) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	_, ok := l.db[key]
	return ok
}

func (l *LocalStorage) GetKeys() (keys []string) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	for i := range l.db {
		keys = append(keys, i)
	}

	return keys
}
