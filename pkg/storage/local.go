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
