package com.reactor.app;

import com.reactor.app.models.Comentarios;
import com.reactor.app.models.Usuario;
import com.reactor.app.models.UsuarioComentarios;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(SpringBootReactorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringBootReactorApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        ejemploContraPresion();

    }


    //En la contrapresion, el suscriptor puede solicitar que le envie menos datos
    public void ejemploContraPresion() {
        Flux.range(1, 10)
                .log() // muestra un log de lo que va sucediendo.
                //.limitRate(2) //cantidad de elementos que se desean recibir por lotes
                .subscribe(new Subscriber<Integer>() {

                    private Subscription s;
                    private final Integer limite = 2;
                    private Integer consumido = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.s = s;
                        s.request(limite);
                    }

                    @Override
                    public void onNext(Integer integer) {
                        log.info(integer.toString());
                        consumido++;
                        if (consumido.equals(limite)) {
                            consumido = 0;
                            s.request(limite);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });
    }


    // Crea desde cero un Observable Flux
    public void ejemploIntervalDesdeCreate() {
        Flux.create(emitter -> {
            Timer timer = new Timer();

            timer.schedule(new TimerTask() {
                private Integer contador = 0;

                @Override
                public void run() {
                    emitter.next(++contador); //se emite el siguiente, preincremente
                    if (contador == 10) {// si llega a 10 para
                        timer.cancel();
                        emitter.complete();
                    }
                    //Provoca un error en 5
                    if (contador == 5) {
                        timer.cancel();
                        emitter.error(new InterruptedException("Error, se ha detenido el flux en 5"));
                    }
                }
            }, 1000, 1000);
        })
                .subscribe(next -> log.info(next.toString()),  //lo que imprime suscribiendo
                        error -> log.error(error.getMessage()),  //manejo de error
                        () -> log.info("Hemos terminado"));  //cuando teermina correctamente envia mensaje
    }


    // Maneja los interval infinito, para que sean mostrados hasta 5
    public void ejemploIntervalInfinito() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        Flux.interval(Duration.ofSeconds(1))
                .doOnTerminate(latch::countDown)
                .flatMap(i -> {
                    if (i >= 5) {
                        return Flux.error(new InterruptedException("Solo hasta 5"));
                    }
                    return Flux.just(i);
                })
                .map(i -> "Hola " + i)
                .retry(2) //Si falla, ejecuta 2 veces mas
                .subscribe(log::info, e -> log.error(e.getMessage()));

        latch.await();
    }


    // Da un retraso de 1 segundo entre cada impresion del log en pantalla
    public void ejemploDelay() throws InterruptedException {
        Flux<Integer> rango = Flux.range(1, 12)
                .delayElements(Duration.ofSeconds(1))
                .doOnNext(i -> log.info(i.toString()));

        rango.blockLast();

        //Thread.sleep(12000);
    }


    // Da un retraso de 1 segundo entre cada impresion del log en pantalla
    public void ejemploInterval() {
        Flux<Integer> rango = Flux.range(1, 12);
        Flux<Long> retraso = Flux.interval(Duration.ofSeconds(1));

        rango.zipWith(retraso, (ra, re) -> ra)
                .doOnNext(i -> log.info(i.toString()))
                .blockLast(); //bloquea para visualizar la ejecucion en segundo plano
    }


    //Usando el operador range
    public void ejemploZipWithRange() {
        Flux<Integer> rangos = Flux.range(0, 4);
        Flux<Integer> valores = Flux.just(1, 2, 3, 4);

        valores.map(i -> (i * 2))
                .zipWith(rangos, (uno, dos) -> String.format("Primer Flux: %d, Segundo Flux: %d", uno, dos))
                .subscribe(log::info);
    }


    //Forma 2 combinando dos flujos con zipwith
    public void ejemploUsuarioComentarioZipWithTwo() {
        // Se crean dos monos observables
        Mono<Usuario> usuarioMono = Mono.fromCallable(this::crearUsuario);

        Mono<Comentarios> comentariosMono = Mono.fromCallable(() -> {
            Comentarios comentarios = new Comentarios();
            comentarios.addComentario("Hola Christian que tal");
            comentarios.addComentario("Yo muy bien programando");
            comentarios.addComentario("tomando el curso spring");
            return comentarios;
        });

        // Combina los dos flujos mono
        Mono<UsuarioComentarios> usuarioConComentarios = usuarioMono
                .zipWith(comentariosMono)
                .map(tuple -> {
                    Usuario u = tuple.getT1();
                    Comentarios c = tuple.getT2();
                    return new UsuarioComentarios(u, c);
                });

        usuarioConComentarios.subscribe(uc -> log.info(uc.toString()));
    }

    //Forma 1 combinando dos flujos con zipwith
    public void ejemploUsuarioComentarioZipWith() {
        // Se crean dos monos observables
        Mono<Usuario> usuarioMono = Mono.fromCallable(this::crearUsuario);

        Mono<Comentarios> comentariosMono = Mono.fromCallable(() -> {
            Comentarios comentarios = new Comentarios();
            comentarios.addComentario("Hola Christian que tal");
            comentarios.addComentario("Yo muy bien programando");
            comentarios.addComentario("tomando el curso spring");
            return comentarios;
        });

        // Combina los dos flujos mono
        Mono<UsuarioComentarios> usuarioConComentarios = usuarioMono
                .zipWith(comentariosMono, UsuarioComentarios::new);

        usuarioConComentarios.subscribe(uc -> log.info(uc.toString()));
    }


    //Funcion se llama para usar en el callable
    public Usuario crearUsuario() {
        return new Usuario("christian", "Rodriguez");
    }

    // Combina dos flujos mono
    public void ejemploUsuarioComentarioFlatMap() {
        // Se crean dos monos observables
        Mono<Usuario> usuarioMono = Mono.fromCallable(this::crearUsuario);

        Mono<Comentarios> comentariosMono = Mono.fromCallable(() -> {
            Comentarios comentarios = new Comentarios();
            comentarios.addComentario("Hola Christian que tal");
            comentarios.addComentario("Yo muy bien programando");
            comentarios.addComentario("tomando el curso spring");
            return comentarios;
        });

        // Combina los dos flujos mono
        Mono<UsuarioComentarios> usuarioConComentarios = usuarioMono
                .flatMap(u -> comentariosMono.map(c -> new UsuarioComentarios(u, c)));

        usuarioConComentarios.subscribe(uc -> log.info(uc.toString()));
    }


    // Convirtiendo un Observables Flux a Mono
    public void ejemploCollectList() {

        List<Usuario> usuariosList = new ArrayList<>();
        usuariosList.add(new Usuario("Andres", "Guzman"));
        usuariosList.add(new Usuario("Carlos", "Carvajal"));
        usuariosList.add(new Usuario("Pedro", "Gonzalez"));
        usuariosList.add(new Usuario("Bruce", "Lee"));
        usuariosList.add(new Usuario("Diana", "Lopez"));
        usuariosList.add(new Usuario("Bruce", "Willis"));


        Mono<List<Usuario>> monoo = Flux.fromIterable(usuariosList)
                .collectList(); //convierte a una lista mono

        monoo.subscribe(lista -> {
            lista.forEach(item -> log.info(item.toString()));
        });

    }


    //convierte un flux list a un flux string
    public void ejemploToString() {

        List<Usuario> usuariosList = new ArrayList<>();
        usuariosList.add(new Usuario("Andres", "Guzman"));
        usuariosList.add(new Usuario("Carlos", "Carvajal"));
        usuariosList.add(new Usuario("Pedro", "Gonzalez"));
        usuariosList.add(new Usuario("Bruce", "Lee"));
        usuariosList.add(new Usuario("Diana", "Lopez"));
        usuariosList.add(new Usuario("Bruce", "Willis"));


        Flux.fromIterable(usuariosList)
                .map(usuario -> usuario.getNombre().toUpperCase().concat(" ").concat(usuario.getApellido().toUpperCase()))// crea un objeto de tipo usuario
                .flatMap(nombre -> {
                    if (nombre.contains("BRUCE")) {  //Si pasa validacion, retorna un flujo mono de usuario
                        return Mono.just(nombre);
                    } else {
                        return Mono.empty();
                    }
                }) // filtra por los bruce
                .map(String::toLowerCase)
                .subscribe(log::info);
    }


    //Usa el operador flatMap
    public void ejemploFlatMap() {

        List<String> usuariosList = new ArrayList<>();
        usuariosList.add("Andres Gomez");
        usuariosList.add("Carlos Carvajal");
        usuariosList.add("Pedro Gonzalez");
        usuariosList.add("Bruce Lee");
        usuariosList.add("Diana Lopez");
        usuariosList.add("Bruce Willis");


        Flux.fromIterable(usuariosList)
                .map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))// crea un objeto de tipo usuario
                .flatMap(usuario -> {
                    if (usuario.getNombre().equalsIgnoreCase("bruce")) {  //Si pasa validacion, retorna un flujo mono de usuario
                        return Mono.just(usuario);
                    } else {
                        return Mono.empty();
                    }
                }) // filtra por los bruce
                .map(usuario -> { // devuelve los objeos de tipo usuario con el nombre en minuscula
                    String nombre = usuario.getNombre().toLowerCase();
                    usuario.setNombre(nombre);
                    return usuario;
                })
                .subscribe(usuario -> log.info(usuario.toString()));
    }


    //Crea un flux observable a atraves de una lista o un iterable
    public void ejemploIterable() throws Exception {

        List<String> usuariosList = new ArrayList<>();
        usuariosList.add("Andres Gomez");
        usuariosList.add("Carlos Carvajal");
        usuariosList.add("Pedro Gonzalez");
        usuariosList.add("Bruce Lee");
        usuariosList.add("Diana Lopez");
        usuariosList.add("Bruce Willis");


        Flux<String> nombres = Flux.fromIterable(usuariosList);

        //Flux<String> nombres = Flux.just("Andres Gomez", "Carlos Carvajal", "Pedro Gonzalez", "Diana Lopez", "Bruce Lee", "Bruce Willis");

        Flux<Usuario> usuarios = nombres.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))// crea un objeto de tipo usuario
                .filter(usuario -> usuario.getNombre().equalsIgnoreCase("Bruce"))// filtra por los bruce
                .doOnNext(usuario -> {//luego hace en el siguiente paso una impresion en terminal
                    if (usuario == null) {
                        throw new RuntimeException("Nombres no pueden ser vacios");
                    }
                    System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
                })
                .map(usuario -> {// devuelve los objeos de tipo usuario con el nombre en minuscula
                    String nombre = usuario.getNombre().toLowerCase();
                    usuario.setNombre(nombre);
                    return usuario;
                });


        // Realiza el subscribe del flujo
        //log::info
        usuarios.subscribe(usuario -> log.info(usuario.toString()), // los suscribe imprimiendolos en logs
                error -> log.error(error.getMessage()),
                new Runnable() { // Corre al terminar
                    @Override
                    public void run() {
                        log.info("Ha finalizado la ejecucion del observable");
                    }
                });
    }
}
