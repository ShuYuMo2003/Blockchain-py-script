from random import randint

rawevents = open('eventsForPertest.txt', 'r').read().split('#')

eventsSlide = []

now = []
for event in rawevents:
	if randint(1, 1000) == 1:
		eventsSlide.append(now)
		now = []
	now.append(event.replace('\n', ' '))

result = eventsSlide[0]
for i in range(1, len(eventsSlide)):
	nowSlide = eventsSlide[i]
	last = eventsSlide[i - 1][-1].replace('\n', ' ').split()[-1]
	result = result + nowSlide + [f'Rollback {last}'] + nowSlide


open('eventsForPerTestFinal.txt', 'w').write('\n#'.join(result))



